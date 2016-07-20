import copy
import gc
import logging
import os

from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net.run import run_nodes, argparser
from bndl.util.log import configure_console_logging
from bndl.net.connection import getlocalhostname


logger = logging.getLogger(__name__)


argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.worker'
argparser.set_defaults(seeds=['tcp://%s:5000' % getlocalhostname()])


class Worker(ExecutionWorker):
    buckets = {}
    broadcast_values_cache = {}
    dset_cache = {}

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


    def uncache_dset(self, src, dset_id):
        if dset_id in self.dset_cache:
            del self.dset_cache[dset_id]


def main():
    configure_console_logging()
    args = argparser.parse_args()
    run_nodes(Worker(addresses=args.listen_addresses, seeds=args.seeds))


if __name__ == '__main__':
    main()
