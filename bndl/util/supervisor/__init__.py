from asyncio.futures import Future
from asyncio.locks import Condition
from asyncio.subprocess import Process
from functools import partial
from subprocess import Popen
import argparse
import asyncio
import atexit
import collections
import copy
import os
import signal
import socket
import sys

from bndl.util import aio
from bndl.util.aio import get_loop
from bndl.util.log import configure_logging
from threading import Thread


def entry_point(string):
    try:
        module, main_method = string.split(':')
        return module, main_method
    except:
        raise ValueError()


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
    def __init__(self, module, main, args):
        self.proc = None
        self.module = module
        self.main = main
        self.args = args
        self.watcher = None


    def start(self):
        if self.running:
            raise RuntimeError("Can't run a child twice")
        self.proc = Popen([sys.executable, '-m', 'bndl.util.supervisor.child', self.module, self.main] + self.args)
        atexit.register(self.terminate)
        self.watcher = Thread(target=self.watch, daemon=True)
        self.watcher.start()

    def watch(self):
        assert self.running
        while True:
            retcode = self.proc.wait()
            if retcode not in (0, signal.SIGTERM, signal.SIGKILL):
                print('restarting process', self.proc.pid, 'which terminated with return code', retcode)
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
    def __init__(self, module, main, args, process_count):
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        self.children = []


    def start(self):
        for _ in range(self.process_count):
            self._start()


    def _start(self):
        child = Child(self.module, self.main, self.args)
        self.children.append(child)
        child.start()


    def stop(self):
        for child in self.children:
            child.terminate()


    def wait(self, timeout=None):
        for child in self.children:
            child.wait(timeout)


def main():
    configure_logging('supervisor-' + '.'.join(map(str, (os.getpid(), socket.getfqdn()))))

    args = argparser.parse_args(split_args())
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
