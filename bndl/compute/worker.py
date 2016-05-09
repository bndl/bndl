import copy
import gc
import logging
import os

from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net.run import argparser, run_nodecls
from bndl.util.log import configure_console_logging


logger = logging.getLogger(__name__)


argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.worker'


class Worker(ExecutionWorker):
    def __init__(self, *args, **kwargs):
        os.environ['PYTHONHASHSEED'] = '0'
        super().__init__(*args, **kwargs)
        self.buckets = {}
        self.broadcast_values = {}
        self.dset_cache = {}


    def get_bucket(self, src, dset_id, part_idx):
        try:
            return self.buckets.get(dset_id, {})[part_idx]
        except KeyError:
            return ()

    def clear_bucket(self, src, dset_id, part_idx=None, local=False):
        try:
            if part_idx is not None:
                del self.buckets.get(dset_id, {})[part_idx]
            else:
                del self.buckets[dset_id]
            gc.collect()
        except KeyError:
            pass


    def unpersist_broadcast_value(self, src, key):
        if key in self.broadcast_values:
            del self.broadcast_values[key]


    def uncache_dset(self, src, dset_id):
        if dset_id in self.dset_cache:
            del self.dset_cache[dset_id]


def main():
    configure_console_logging()
    args = argparser.parse_args()
    run_nodecls(Worker, args)


if __name__ == '__main__':
    main()
