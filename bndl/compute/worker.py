import argparse
import copy
import gc
import logging
import os

from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net.connection import getlocalhostname
from bndl.net.run import run_nodes, argparser
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.supervisor import split_args, Supervisor


logger = logging.getLogger(__name__)


argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.worker'


class Worker(ExecutionWorker):
    buckets = {}
    broadcast_values_cache = {}

    def __init__(self, *args, **kwargs):
        os.environ['PYTHONHASHSEED'] = '0'
        super().__init__(*args, **kwargs)


    def get_bucket(self, src, dset_id, part_idx):
        try:
            return self.buckets.get(dset_id, {})[part_idx]
        except KeyError:
            return ()

    def clear_bucket(self, src, dset_id, part_idx=None):
        try:
            if part_idx is not None:
                del self.buckets.get(dset_id, {})[part_idx]
            else:
                del self.buckets[dset_id]
            gc.collect()
        except KeyError:
            pass


    def unpersist_broadcast_value(self, src, key):
        if key in self.broadcast_values_cache:
            del self.broadcast_values_cache[key]


def main():
    conf = Config()

    args = argparser.parse_args()
    listen_addresses = args.listen_addresses or conf.get('bndl.net.listen_addresses')
    seeds = args.seeds or conf.get('bndl.net.seeds') or ['tcp://%s:5000' % getlocalhostname()]

    run_nodes(Worker(addresses=listen_addresses, seeds=seeds))


def run_workers():
    supervisor_args, worker_args = split_args()
    argparser = argparse.ArgumentParser()
    argparser.add_argument('worker_count', nargs='?', type=int, default=os.cpu_count() or 1)
    args = argparser.parse_args(supervisor_args)
    supervisor = Supervisor('bndl.compute.worker', 'main', worker_args, args.worker_count)
    supervisor.start()
    try:
        supervisor.wait()
    except KeyboardInterrupt:
        with catch():
            supervisor.stop()


if __name__ == '__main__':
    main()
