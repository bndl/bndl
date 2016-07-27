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

from bndl.util.log import configure_logging


logger = logging.getLogger(__name__)


# Environment variable to indicate which child the process is. It's value is
# formatted as {supervisor.id}.{child.id}
CHILD_ID = 'BNDL_SUPERVISOR_CHILD'

# If a supervised child fails within MIN_RUN_TIME seconds, the child is
# considered unstable and isn't rebooted.
MIN_RUN_TIME = 10


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
        self.watcher = Thread(target=self.watch, daemon=True,
                              name='bndl-supervisor-watcher-%s' % self.id)
        self.watcher.start()


    def watch(self):
        '''
        Watch the child process and restart it if it stops unexpectedly.

            - if the return code is 0 we assume the child wanted to exit
            - if the return code is SIGTERM or SIGKILL we assume an operator
              wants the child to exit or it's parent (the supervisor) sent
              SIGKILL, see Child.terminate
            - if the returncode is something else, restart the process
            - unless it was started < MIN_RUN_TIME (we consider the failure not
              transient)
        '''
        assert self.running
        retcode = self.proc.wait()
        logger.info('Child %s (%s:%s) exited with code %s', self.id , self.module, self.main, retcode)

        if retcode not in (0, signal.SIGTERM, signal.SIGKILL, -signal.SIGTERM, -signal.SIGKILL) \
           and (time.time() - self.started_on) > MIN_RUN_TIME:
            self.start()

    @property
    def running(self):
        return self.proc and self.proc.returncode is None


    def terminate(self):
        if self.running:
            try:
                logger.info('Terminating child %s (%s:%s) with SIGTERM', self.id , self.module, self.main)
                self.proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass  # already terminated


    def wait(self, timeout):
        if self.running:
            self.proc.wait(timeout)



class Supervisor(object):
    _ids = itertools.count()

    def __init__(self, module, main, args, process_count=None):
        self.id = next(Supervisor._ids)
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        self.children = []


    def start(self):
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


def main(supervisor_args=None, child_args=None):
    configure_logging('supervisor-' + '.'.join(map(str, (os.getpid(), socket.getfqdn()))))

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
        print('supervisor is starting', supervisor_args.process_count, entry_point, 'child processes ...')
        supervisor.start()
        supervisor.wait()
    except KeyboardInterrupt:
        print('supervisor interrupted')
    finally:
        print('supervisor is stopping', supervisor_args.process_count, entry_point, 'child processes')
        supervisor.stop()


if __name__ == '__main__':
    main()
