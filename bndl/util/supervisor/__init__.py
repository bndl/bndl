from subprocess import Popen
from threading import Thread
import argparse
import atexit
import itertools
import logging
import os
import signal
import socket
import sys
import time


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


argparser = argparse.ArgumentParser()
argparser.epilog = 'Use -- after the supervisor arguments to separate them from' \
                   'arguments to the main program.'
argparser.add_argument('entry_point', type=entry_point)
argparser.add_argument('--process_count', nargs='?', type=int, default=os.cpu_count())


def split_args():
    idx = -1
    for idx, val in enumerate(sys.argv):
        if val == '--':
            break

    return sys.argv[1:idx + 1], sys.argv[idx + 1:]


class Child(object):
    def __init__(self, child_id, module, main, args):
        self.id = child_id
        self.module = module
        self.main = main
        self.args = args

        self.proc = None
        self.watcher = None
        self.started_on = None


    def start(self):
        if self.running:
            raise RuntimeError("Can't run a child twice")

        logger.info('Starting child %s (%s:%s)', self.id , self.module, self.main)

        env = {CHILD_ID:str(self.id)}
        env.update(os.environ)
        args = [sys.executable, '-m', 'bndl.util.supervisor.child', self.module, self.main] + self.args
        self.proc = Popen(args, env=env)
        self.started_on = time.time()

        atexit.register(self.terminate)


    @property
    def returncode(self):
        return self.proc.poll() if self.proc else None


    @property
    def running(self):
        return self.proc and self.returncode is None


    def terminate(self):
        if self.running:
            try:
                logger.info('Terminating child %s (%s:%s) with SIGTERM', self.id , self.module, self.main)
                self.proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass  # already terminated


    def wait(self, timeout=None):
        if self.running:
            self.proc.wait(timeout)
            return self.proc.returncode



class Supervisor(object):
    _ids = itertools.count()

    def __init__(self, module, main, args, process_count=None):
        self.id = next(Supervisor._ids)
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        self.children = []
        self._watcher = Thread(target=self._watch, daemon=True,
                              name='bndl-supervisor-watcher-%s' % self.id)

    def start(self):
        self._watcher.start()
        for _ in range(self.process_count):
            self._start()


    def _start(self):
        child_id = '%s.%s' % (self.id, len(self.children))
        child = Child(child_id, self.module, self.main, self.args)
        self.children.append(child)
        child.start()


    def stop(self):
        for child in self.children:
            child.terminate()


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
        while True:
            if self.children:
                check_interval = CHECK_INTERVAL / len(self.children)
                for child in self.children:
                    returncode = child.returncode
                    if returncode is not None:
                        logger.info('Child %s (%s:%s) exited with code %s', child.id , child.module, child.main, returncode)
                        if returncode not in DNR_CODES and (time.time() - child.started_on) > MIN_RUN_TIME:
                            child.start()
                    else:
                        time.sleep(check_interval)
            else:
                time.sleep(CHECK_INTERVAL)


def echo(*args):
    if __name__ == '__main__':
        print(*args)


def main(supervisor_args=None, child_args=None):
    # parse arguments
    if supervisor_args is None:
        supervisor_args, prog_args = split_args()
        supervisor_args = argparser.parse_args(supervisor_args)
    else:
        prog_args = child_args or []

    # create the supervisor
    supervisor = Supervisor(supervisor_args.entry_point[0], supervisor_args.entry_point[1], prog_args, supervisor_args.process_count)

    # and run until CTRL-C / SIGTERM
    try:
        entry_point = ':'.join(supervisor_args.entry_point)
        echo('supervisor is starting', supervisor_args.process_count, entry_point, 'child processes ...')
        supervisor.start()
        supervisor.wait()
    except KeyboardInterrupt:
        echo('supervisor interrupted')
    finally:
        echo('supervisor is stopping', supervisor_args.process_count, entry_point, 'child processes')
        supervisor.stop()


if __name__ == '__main__':
    main()
