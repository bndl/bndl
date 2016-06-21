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
        bndl_args = sys.argv[1:]
        sys.argv = sys.argv[:1]
    return bndl_args


class Monitor(asyncio.protocols.SubprocessProtocol):
    def __init__(self, supervisor):
        self.supervisor = supervisor
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        print('connection lost, reason:', exc)

    def process_exited(self):
        pid = self.transport.get_pid()
        returncode = self.transport.get_returncode()
        print('process', pid, 'exited with return code', returncode)
        if returncode != -signal.SIGTERM:
            aio.run_coroutine_threadsafe(self.supervisor._start(), self.supervisor.loop)

    def pipe_data_received(self, fd, data):
        pid = self.transport.get_pid()
        for line in data.decode('utf-8').strip().split('\n'):
            print(pid, ':', line)


SCRIPT = '''
# {mod}.{main} (BNDL supervised process)

import signal
signal.signal(signal.SIGINT, signal.SIG_IGN)

import {mod}
{mod}.{main}()
'''


class Supervisor(object):
    def __init__(self, loop, module, main, args, process_count):
        self.loop = loop
        self.module = module
        self.main = main
        self.args = args
        self.process_count = process_count
        self.subprocesses = []


    @asyncio.coroutine
    def start(self):
        for _ in range(self.process_count):
            yield from self._start()


    @asyncio.coroutine
    def _start(self):
        args = [sys.executable, '-m', 'bndl.util.supervisor.child', self.module, self.main] + self.args

        env = copy.copy(os.environ)
        env['PYTHONHASHSEED'] = '0'

        transport, protocol = yield from self.loop.subprocess_exec(
            lambda: Monitor(self),
            *args,
            stderr=asyncio.subprocess.STDOUT,
            env=env
        )

        self.subprocesses.append((transport, protocol))

        return protocol


    @asyncio.coroutine
    def stop(self):
        for transport, protocol in self.subprocesses:  # @UnusedVariable
            transport.send_signal(signal.SIGTERM)


def main():
    args = argparser.parse_args(split_args())

    loop = get_loop()

    supervisor = Supervisor(loop, args.entry_point[0], args.entry_point[1], sys.argv[1:], args.process_count)
    configure_logging('supervisor-' + '.'.join(map(str, (os.getpid(), socket.getfqdn()))))

    try:
        loop.run_until_complete(supervisor.start())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(supervisor.stop())
    loop.close()

if __name__ == '__main__':
    main()
