# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import atexit
import itertools
import logging
import os
import signal
import subprocess
import sys
import threading
import time

import bndl


logger = logging.getLogger(__name__)


# Environment variable to indicate which child the process is. It's value is
# formatted as {supervisor.id}.{child.id}
CHILD_ID = 'BNDL_SUPERVISOR_CHILD'

# Every CHECK_INTERVAL seconds supervised children are checked for exit codes
# and restarted if necessary.
CHECK_INTERVAL = 1


# If a supervised child fails within MIN_RUN_TIME seconds, the child is
# considered unstable and isn't rebooted.
MIN_RUN_TIME = 10

# When a child exits with one of the following return codes, it is not revived.
# 0 indicates the process exited 'normally'
# A process may be killed with SIGTERM or SIGKILL, and exit with -SIGTERM /
# -SIGKILL, this is considered an action by a parent process and/or
# administrator.
# The convention is maintained that if a child wants to perform cleanup (e.g.
# run atexit functionality) it exists with the SIGTERM or SIGKILL codes.
# Note that also the time between exit and the last start is also taken into
# account, see Supervisor._watch.
DNR_CODES = 0, -signal.SIGTERM, -signal.SIGKILL, signal.SIGTERM, signal.SIGKILL


def entry_point(string):
    if ':' in string:
        try:
            module, main_method = string.split(':')
            return module, main_method
        except Exception as e:
            raise ValueError('Unable to parse entry point "%s". Entry points must be formatted as'
                             ' module:method (e.g. your.module[:main])' % string) from e
    else:
        return string, 'main'


base_argparser = argparse.ArgumentParser(add_help=False)
base_argparser.epilog = 'Use -- after the supervisor arguments to separate them from' \
                   'arguments to the main program.'

base_argparser.add_argument('--numactl', dest='numactl', action='store_true',
                            help='Bind processes round-robin to NUMA zones with numactl.')
base_argparser.add_argument('--no-numactl', dest='numactl', action='store_false',
                            help='Don\'t attempt to bind processes to a NUMA zone with numactl')

base_argparser.add_argument('--pincore', dest='pincore', action='store_true',
                            help='Pin processes round-robin to a specific core with taskset')
base_argparser.add_argument('--no-pincore', dest='pincore', action='store_false',
                            help='Don\'t pin processes to specific cores with taskset')

base_argparser.add_argument('--jemalloc', dest='jemalloc', action='store_true',
                            help='Use jemalloc.sh (if available on the PATH).')
base_argparser.add_argument('--no-jemalloc', dest='jemalloc', action='store_false',
                            help='Don\'t attempt to use jemalloc.sh (use the system default '
                                 'malloc, usually malloc from glibc).')

base_argparser.set_defaults(numactl=bndl.conf['bndl.run.numactl'],
                            pincore=bndl.conf['bndl.run.pincore'],
                            jemalloc=bndl.conf['bndl.run.jemalloc'])


main_argparser = argparse.ArgumentParser(parents=[base_argparser])
main_argparser.add_argument('--process_count', nargs='?', type=int, default=os.cpu_count())
main_argparser.add_argument('entry_point', type=entry_point)


def split_args():
    idx = -1
    for idx, val in enumerate(sys.argv):
        if val == '--':
            break

    return sys.argv[1:idx + 1], sys.argv[idx + 1:]


def _check_command_exists(name):
    try:
        subprocess.check_output(['which', name], stderr=subprocess.DEVNULL)
    except Exception:
        return False
    else:
        return True



class Child(object):
    def __init__(self, child_id, module, main, args, numactl, pincore, jemalloc):
        self.id = child_id
        self.module = module
        self.main = main
        self.args = args

        self.proc = None
        self.pid = None
        self.watcher = None
        self.started_on = None

        self.executable = [sys.executable]

        numactl &= _check_command_exists('numactl') and _check_command_exists('numastat')
        jemalloc &= _check_command_exists('jemalloc.sh')
        pincore &= _check_command_exists('taskset')

        if numactl and not pincore:
            n_zones = len(subprocess.check_output(['numastat | head -n 1'], shell=True).split())
            if n_zones > 1:
                node = str(sum(self.id) % n_zones)
                self.executable = ['numactl', '-N', node, '--preferred', node] + self.executable

        if pincore:
            n_cores = os.cpu_count()
            if n_cores > 1:
                core = str(sum(self.id) % n_cores)
                self.executable = ['taskset', '-c', core] + self.executable

        if jemalloc:
            self.executable = ['jemalloc.sh'] + self.executable


    def start(self):
        if self.running:
            raise RuntimeError("Can't run a child twice")

        child_id = '.'.join(map(str, self.id))
        env = {CHILD_ID: child_id}
        env.update(os.environ)
        env['PYTHONHASHSEED'] = '0'

        logger.info('Starting child %s (%s:%s)', child_id, self.module, self.main)

        args = self.executable \
               + ['-m', 'bndl.run.child', self.module, self.main] \
               + list(self.args)


        self.proc = subprocess.Popen(args, env=env)
        self.pid = self.proc.pid
        self.started_on = time.time()

        atexit.register(self.terminate)


    @property
    def returncode(self):
        return self.proc.poll() if self.proc else None


    @property
    def running(self):
        return self.proc and self.returncode is None


    def terminate(self, force=False):
        if self.running:
            try:
                logger.info('Terminating child %s (%s:%s) with SIGTERM', self.id , self.module, self.main)
                self.proc.send_signal(signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass  # already terminated


    def wait(self, timeout=None):
        if self.running:
            self.proc.wait(timeout)
            return self.proc.returncode



class Supervisor(object):
    _ids = itertools.count()

    def __init__(self, module, main, args, process_count=None,
                 numactl=None, pincore=None, jemalloc=None,
                 min_run_time=MIN_RUN_TIME, check_interval=CHECK_INTERVAL):
        self.id = next(Supervisor._ids)
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        
        if numactl is None:
            numactl = bndl.conf['bndl.run.numactl']
        if pincore is None:
            pincore = bndl.conf['bndl.run.pincore']
        if jemalloc is None:
            jemalloc = bndl.conf['bndl.run.jemalloc']

        self.numactl = numactl
        self.pincore = pincore
        self.jemalloc = jemalloc
        
        self.children = []
        self.min_run_time = min_run_time
        self.check_interval = check_interval
        self._watcher = threading.Thread(target=self._watch, daemon=True,
                                         name='bndl-supervisor-%s' % self.id)


    def start(self):
        self._watcher.start()
        for _ in range(self.process_count):
            self._start()


    def _start(self):
        child_id = (self.id, len(self.children))
        child = Child(child_id, self.module, self.main, self.args,
                      numactl=self.numactl, pincore=self.pincore, jemalloc=self.jemalloc)
        self.children.append(child)
        child.start()


    def stop(self):
        self._watcher = None  # signals watcher to stop
        for child in self.children:
            child.terminate()
        for child in self.children:
            try:
                child.wait(MIN_RUN_TIME)
            except subprocess.TimeoutExpired:
                child.terminate(True)


    def wait(self, timeout=None):
        start = time.time()
        for child in self.children:
            child.wait((timeout - (time.time() - start)) if timeout else None)


    def _watch(self):
        '''
        Watch the child processes and restart if one stops unexpectedly.

            - if the return code is 0 we assume the child wanted to exit
            - if the return code is SIGTERM or SIGKILL we assume an operator
              wants the child to exit or it's parent (the supervisor) sent
              SIGKILL, see Child.terminate
            - if the return code is something else, restart the process
            - unless it was started < MIN_RUN_TIME (we consider the failure not
              transient)
        '''
        while self._watcher == threading.current_thread():
            if self.children:
                check_interval = self.check_interval / len(self.children)
                for child in self.children:
                    returncode = child.returncode
                    if returncode is not None:
                        restart = returncode not in DNR_CODES and (time.time() - child.started_on) > self.min_run_time
                        logger.log(logging.ERROR if restart else logging.INFO,
                                   'Child %s (%s:%s, pid %s) exited with code %s',
                                   child.id , child.module, child.main, child.pid, returncode)
                        if restart:
                            child.start()
                    else:
                        time.sleep(check_interval)
            else:
                time.sleep(self.check_interval)


    @classmethod
    def from_args(cls, args, prog_args=()):
        return cls(
            args.entry_point[0],
            args.entry_point[1],
            prog_args,
            args.process_count,
            args.numactl,
            args.pincore,
            args.jemalloc
        )



def main(supervisor_args=None, child_args=None):
    # parse arguments
    if supervisor_args is None:
        supervisor_args, prog_args = split_args()
        supervisor_args = main_argparser.parse_args(supervisor_args)
    else:
        prog_args = child_args or []

    # create the supervisor
    supervisor = Supervisor.from_args(supervisor_args, prog_args)

    # and run until CTRL-C / SIGTERM
    try:
        supervisor.start()
        supervisor.wait()
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()


if __name__ == '__main__':
    main()
