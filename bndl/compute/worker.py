import argparse
import logging
import os

from bndl.compute import broadcast
from bndl.compute.blocks import BlockManager
from bndl.compute.shuffle import ShuffleManager
from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net import run
from bndl.net.connection import getlocalhostname
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.supervisor import Supervisor


logger = logging.getLogger(__name__)


class Worker(ExecutionWorker, BlockManager, ShuffleManager):
    def __init__(self, *args, **kwargs):
        os.environ['PYTHONHASHSEED'] = '0'
        ExecutionWorker.__init__(self, *args, **kwargs)
        BlockManager.__init__(self)
        ShuffleManager.__init__(self)


    def unpersist_broadcast_values(self, src, name):
        self.remove_blocks(name)
        del broadcast.download_coordinator[name]


def main():
    conf = Config()

    args = argparse.ArgumentParser(parents=[run.argparser]).parse_args()
    listen_addresses = args.listen_addresses or conf.get('bndl.net.listen_addresses')
    seeds = args.seeds or conf.get('bndl.net.seeds') or ['tcp://%s:5000' % getlocalhostname()]

    run.run_nodes(Worker(addresses=listen_addresses, seeds=seeds))


def run_workers():
    def add_worker_count(parser):
        parser.add_argument('worker_count', nargs='?', type=int, default=os.cpu_count() or 1)
    # use a parser with run.argparser as parent to get correct argument parsing / help message
    argparser = argparse.ArgumentParser(parents=[run.argparser])
    add_worker_count(argparser)
    args = argparser.parse_args()

    # reconstruct the arguments for the worker
    # parse_known_args doesn't take out the worker_count positional argument correctly
    worker_args = []
    if args.listen_addresses:
        worker_args += ['--listen-addresses'] + args.listen_addresses
    if args.seeds:
        worker_args += ['--seeds'] + args.seeds

    supervisor = Supervisor('bndl.compute.worker', 'main', worker_args, args.worker_count)
    supervisor.start()
    try:
        supervisor.wait()
    except KeyboardInterrupt:
        with catch():
            supervisor.stop()


if __name__ == '__main__':
    main()
