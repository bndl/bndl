import argparse
import asyncio
import copy
import os
import socket
import sys

from bndl.util.aio import get_loop
from bndl.util.log import configure_logging
from bndl.util import aio
import signal
from asyncio.locks import Condition
from asyncio.subprocess import Process
import atexit
import collections
from asyncio.futures import Future
from subprocess import Popen
from functools import partial


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
        bndl_args = []  # sys.argv[1:]
        # sys.argv = sys.argv[:1]
    return bndl_args


def _proc_exit(proc):
    if proc.returncode is None:
        proc.send_signal(signal.SIGTERM)


class Supervisor(object):
    def __init__(self, module, main, args, process_count):
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        self.subprocesses = []


    def start(self):
        for _ in range(self.process_count):
            self._start()


    def _start(self):
        proc = Popen([sys.executable, '-m', 'bndl.util.supervisor.child', self.module, self.main] + self.args)
        atexit.register(partial(_proc_exit, proc))
        self.subprocesses.append(proc)
        return proc


    def stop(self):
        for proc in self.subprocesses:
            _proc_exit(proc)


    def wait(self, timeout=None):
        for proc in self.subprocesses:
            proc.wait(timeout)


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
