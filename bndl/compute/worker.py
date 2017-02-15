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
import asyncio
import logging
import os

from bndl.compute.blocks import BlockManager
from bndl.compute.broadcast import BroadcastManager
from bndl.compute.memory import LocalMemoryManager, MemorySupervisor
from bndl.compute.shuffle import ShuffleManager
from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net import run
from bndl.net.connection import getlocalhostname
from bndl.run import supervisor
from bndl.util.exceptions import catch
import bndl
from bndl.util.threads import dump_threads
import signal


logger = logging.getLogger(__name__)


class Worker(ExecutionWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.services['blocks'] = BlockManager(self)
        self.services['broadcast'] = BroadcastManager(self)
        self.services['shuffle'] = ShuffleManager(self)
        self.memory = LocalMemoryManager()


    @asyncio.coroutine
    def start(self):
        self.memory.start()
        return (yield from super().start())


    @asyncio.coroutine
    def stop(self):
        self.memory.stop()
        return (yield from super().stop())


main_argparser = argparse.ArgumentParser(parents=[run.argparser])
many_argparser = argparse.ArgumentParser(parents=[run.argparser, supervisor.base_argparser], add_help=False)


def main():
    signal.signal(signal.SIGUSR1, dump_threads)

    conf = bndl.conf
    args = main_argparser.parse_args()
    listen_addresses = args.listen_addresses or conf.get('bndl.net.listen_addresses')
    seeds = args.seeds or conf.get('bndl.net.seeds') or ['tcp://%s:5000' % getlocalhostname()]
    worker = Worker(addresses=listen_addresses, seeds=seeds)
    from __main__ import control_node
    control_node.services['memory'] = worker.memory
    run.run_nodes(worker)



class WorkerSupervisor(supervisor.Supervisor):
    def __init__(self, *args, **kwargs):
        super().__init__('bndl.compute.worker', 'main', *args, **kwargs)
        self.memory = MemorySupervisor(self.rmi)

    def start(self):
        super().start()
        self.memory.start()

    def stop(self):
        self.memory.stop()
        super().stop()

    @classmethod
    def from_args(cls, args, prog_args=()):
        return cls(
            prog_args,
            args.process_count,
            args.numactl,
            args.pincore,
            args.jemalloc
        )


def run_workers():
    signal.signal(signal.SIGUSR1, dump_threads)

    argparser = argparse.ArgumentParser(parents=[many_argparser])

    conf = bndl.conf
    def_worker_count = conf.get('bndl.compute.worker_count') or os.cpu_count() or 1
    argparser.add_argument('process_count', nargs='?', type=int, default=def_worker_count,
                            metavar='worker count', help='The number of workers to start (defaults'
                                                         ' to %s).' % def_worker_count)
    args = argparser.parse_args()

    # reconstruct the arguments for the worker
    # parse_known_args doesn't take out the worker_count positional argument correctly
    worker_args = []
    if args.listen_addresses:
        worker_args += ['--listen-addresses'] + args.listen_addresses
    if args.seeds:
        worker_args += ['--seeds'] + args.seeds

    superv = WorkerSupervisor.from_args(args, worker_args)
    superv.start()
    try:
        superv.wait()
    except KeyboardInterrupt:
        with catch():
            superv.stop()


if __name__ == '__main__':
    main()
