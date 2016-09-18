from collections import defaultdict
import argparse
import gc
import logging
import os

from bndl.compute import broadcast
from bndl.execute.worker import Worker as ExecutionWorker
from bndl.net import run
from bndl.net.connection import getlocalhostname
from bndl.rmi.blocks import BlockManager
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.supervisor import Supervisor


logger = logging.getLogger(__name__)


class Worker(ExecutionWorker, BlockManager):
    def __init__(self, *args, **kwargs):
        os.environ['PYTHONHASHSEED'] = '0'
        ExecutionWorker.__init__(self, *args, **kwargs)
        BlockManager.__init__(self)
        self.buckets = defaultdict(dict)


    def get_bucket_size(self, src, dset_id, bucket_idx):
        try:
            bucket = self.buckets.get(dset_id)[bucket_idx]
            bucket.spill()
            sizes = [len(batch) for batch in bucket.batches]
            return sizes
        except (TypeError, IndexError):
            return ()


    def get_bucket_block(self, src, dset_id, part_idx, batch_idx, block_idx):
        try:
            bucket = self.buckets.get(dset_id)[part_idx]
        except TypeError:
            raise KeyError('No buckets for dataset %r' % dset_id)
        except IndexError:
            raise KeyError('No partition %r in dataset %r' % (part_idx, dset_id))

        try:
            batch = bucket.batches[batch_idx]
        except IndexError:
            raise KeyError('No batch %r in partition %r of dataset %r' % (
                           batch_idx, part_idx, dset_id))

        try:
            return batch[block_idx]
        except IndexError:
            raise KeyError('No block %r in batch %r of partition %r in dataset %r' % (
                           block_idx, batch_idx, part_idx, dset_id))



    def clear_bucket(self, src, dset_id, part_idx=None):
        try:
            if part_idx is not None:
                buckets = self.buckets.get(dset_id)
                buckets[part_idx].clear()
                del buckets[part_idx]
            else:
                for bucket in self.buckets[dset_id]:
                    bucket.clear()
                del self.buckets[dset_id]
            gc.collect()
        except KeyError:
            pass


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
