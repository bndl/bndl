from asyncio.futures import Future
from asyncio.locks import Condition
from asyncio.subprocess import Process
from functools import partial
from subprocess import Popen
from threading import Thread
import argparse
import asyncio
import atexit
import collections
import copy
import itertools
import os
import signal
import socket
import sys
import time

from bndl.util import aio
from bndl.util.aio import get_loop
from bndl.util.log import configure_logging


# Environment variable to indicate which child the process is. It's value is
# formatted as {supervisor.id}.{child.id}
CHILD_ID = 'BNDL_CHILD_ID'

# If a supervised child fails within MIN_RUN_TIME seconds, the child is
# considered unstable and isn't rebooted.
MIN_RUN_TIME = 1


def entry_point(string):
    try:
        module, main_method = string.split(':')
        return module, main_method
    except Exception as e:
        raise ValueError('Unable to parse entry point "%s". Entry points must be formatted as'
                         ' module:method (e.g. your.module:main)' % string) from e


argparser = argparse.ArgumentParser()
argparser.epilog = 'Use -- before bndl arguments to separate the from' \
                   'arguments to the main program.'
argparser.add_argument('entry_point', type=entry_point)
argparser.add_argument('--process_count', nargs='?', type=int, default=os.cpu_count())


def split_args():
    prog_args = []

    idx = -1
    split = False
    for idx, val in enumerate(sys.argv[1:]):
        if val == '--':
            split = True
            break
        else:
            prog_args.append(val)

    if split:
        bndl_args = sys.argv[idx + 2:]
        sys.argv = sys.argv[:1] + prog_args
    else:
        bndl_args = []
    return bndl_args


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

        env = {CHILD_ID:str(self.id)}
        env.update(os.environ)
        args = [sys.executable, '-m', 'bndl.util.supervisor.child', self.module, self.main] + self.args
        self.proc = Popen(args, env=env)
        self.started_on = time.time()

        atexit.register(self.terminate)
        self.watcher = Thread(target=self.watch, daemon=True)
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
        if retcode not in (0, signal.SIGTERM, signal.SIGKILL) and (time.time() - self.started_on) > MIN_RUN_TIME:
            self.start()

    @property
    def running(self):
        return self.proc and self.proc.returncode is None


    def terminate(self):
        if self.running:
            self.proc.send_signal(signal.SIGTERM)


    def wait(self, timeout):
        if self.running:
            self.proc.wait(timeout)



class Supervisor(object):
    _ids = itertools.count()

    def __init__(self, module, main, args, process_count):
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


def main(args=None):
    configure_logging('supervisor-' + '.'.join(map(str, (os.getpid(), socket.getfqdn()))))

    args = args or argparser.parse_args(split_args())
    supervisor = Supervisor(args.entry_point[0], args.entry_point[1], sys.argv[1:], args.process_count)

    try:
        supervisor.start()
        supervisor.wait()
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()
        # supervisor.wait()


if __name__ == '__main__':
    main()
