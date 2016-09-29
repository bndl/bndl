from bisect import bisect_left
from concurrent.futures._base import Future
from math import ceil
from statistics import mean
import gc
import logging
import threading

from bndl.compute.dataset import Dataset, Partition
from bndl.compute.storage import StorageContainerFactory
from bndl.util.collection import batch as batch_data, ensure_collection
from bndl.util.conf import Int, Float
from bndl.util.hash import portable_hash
from cytoolz.functoolz import compose
from cytoolz.itertoolz import merge_sorted
from psutil import virtual_memory


logger = logging.getLogger(__name__)


max_mem_pct = Int(75)
block_size_mb = Float(10)


BLOCK_FRACTION = 4


class Bucket:
    sorted = False

    def __init__(self, idx, key, comb, block_size_mb, default_container):
        self.idx = idx
        self.key = key
        self.comb = comb
        self.block_size_mb = block_size_mb
        self.default_container = default_container
        self.batches = []
        self._spill_lock = threading.Lock()
        self.record_size = None


    @property
    def _block_size_recs(self):
        return ceil(self.block_size_mb * 1024 * 1024 / self.record_size)


    def spill(self, container=None):
        with self._spill_lock:
            if len(self) == 0:
                return

            if not container:
                container = self.default_container

            # prepare blocks
            batch = []
            add_block = batch.append
            batch_no = len(self.batches)
            self.batches.append(batch)

            # apply combiner if any
            data = ensure_collection(self.comb(self)) if self.comb else list(self)
            idx = self.idx

            if not self.record_size:
                test_set = data[:max(3, len(data) // 10)]
                c = self.default_container(('tmp',))
                c.write(test_set)
                self.record_size = c.size / len(test_set)

            if self._block_size_recs > len(data):
                blocks = [data]
            else:
                blocks = batch_data(data, self._block_size_recs)
            for block in blocks:
                c = container(idx + ('%r.%r' % (batch_no, len(batch)),))
                c.write(block)
                add_block(c)

            self.record_size = self.record_size / 2 + mean(block.size for block in batch) / 2

            logger.debug('spilled bucket %r into %r blocks, record size is estimated at %r',
                         idx, len(batch), self.record_size)

            # clear the in memory
            self.clear()


    def __del__(self):
        for batch in self.batches:
            batch.clear()
        self.batches.clear()



class SetBucket(Bucket, set):
    pass



class DictBucket(Bucket, dict):
    def add(self, value):
        self[self.key(value)] = value

    def __iter__(self):
        return iter(self.values())



class ListBucket(Bucket, list):
    add = list.append



class SortedBucket(ListBucket):
    sorted = True

    def spill(self, container=None):
        self.sort(key=self.key)
        super().spill(container)



class RangePartitioner():
    def __init__(self, boundaries, reverse=False):
        self.boundaries = boundaries
        self.reverse = reverse

    def __call__(self, value):
        boundaries = self.boundaries
        boundary = bisect_left(self.boundaries, value)
        return len(boundaries) - boundary if self.reverse else boundary



class ShuffleWritingDataset(Dataset):
    def __init__(self, ctx, src, pcount, partitioner=None, bucket=None, key=None, comb=None,
            max_mem_pct=None, block_size_mb=None, serialization='pickle', compression=None):
        super().__init__(ctx, src)
        self.pcount = pcount or self.ctx.default_pcount
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = bucket or SortedBucket
        self.key = key

        self.max_mem_pct = max_mem_pct or ctx.conf['bndl.compute.shuffle.max_mem_pct']
        self.block_size_mb = block_size_mb or ctx.conf['bndl.compute.shuffle.block_size_mb']
        self.serialization = serialization
        self.compression = compression


    @property
    def sync_required(self):
        return True


    @property
    def cleanup(self):
        def _cleanup(job):
            futures = [worker.clear_bucket(self.id) for worker in job.ctx.workers]
            for future in futures:
                try:
                    future.result()
                except Exception:
                    logger.warning('unable to cleanup after job for shuffle writing dataset %s', self.id, exc_info=True)

        return _cleanup


    def parts(self):
        return [
            ShuffleWritingPartition(self, i, p)
            for i, p in enumerate(self.src.parts())
        ]



class ShuffleWritingPartition(Partition):
    def __init__(self, dset, idx, src):
        super().__init__(dset, idx, src)
        self._disk_container = StorageContainerFactory('disk',
                                                       self.dset.serialization,
                                                       self.dset.compression)
        self._memory_container = StorageContainerFactory('memory',
                                                         self.dset.serialization,
                                                         self.dset.compression)


    def _ensure_buckets(self):
        dset_id = self.dset.id
        part_id = self.idx

        worker = self.dset.ctx.node
        buckets = worker.buckets.get(dset_id)

        if not buckets:
            buckets = [self.dset.bucket((dset_id, part_id, idx), self.dset.key, self.dset.comb,
                                        self.dset.block_size_mb, self._memory_container)
                       for idx in range(self.dset.pcount)]
            worker.buckets[self.dset.id] = buckets
        return buckets


    def _materialize(self, ctx):
        buckets = self._ensure_buckets()
        bucketadd = [bucket.add for bucket in buckets]
        bucket_count = len(buckets)

        key = self.dset.key

        if key:
            partitioner = self.dset.partitioner
            def part_(element):
                return partitioner(key(element))
        else:
            part_ = self.dset.partitioner

        vmem = virtual_memory()

        # the relative amount of memory used to consider as ceiling
        # spill when virtual_memory().percent > max_mem_pct
        max_mem_pct = self.dset.max_mem_pct

        # check every once in a while
        # every percent of total memory or 100 megs
        check_interval_min_mem = -max(100 * 1024 * 1024, vmem.total // 100)
        check_interval_max_mem = check_interval_min_mem * 2
        # because we don't know on forehand how memory usage is growing,
        check_interval = 10

        disk_container = self._disk_container

        check_loop = 0
        for element in self.src.materialize(ctx):
            bucketadd[part_(element) % bucket_count](element)

            check_loop += 1
            if check_loop == check_interval:
                check_loop = 0
                vmem_prev = vmem
                vmem = virtual_memory()
                if vmem.percent > max_mem_pct:
                    for bucket in sorted(buckets, key=len, reverse=True):
                        bucket.spill(disk_container)
                        gc.collect()
                        if virtual_memory().percent < max_mem_pct:
                            break
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('memory usage from %r%% down to %r%% after spilling',
                                     vmem.percent, virtual_memory().percent)
                dt_available = vmem.available - vmem_prev.available
                if dt_available >= check_interval_min_mem:
                    check_interval <<= 1
                elif dt_available <= check_interval_max_mem:
                    check_interval = max(1, check_interval >> 1)



class ShuffleReadingDataset(Dataset):
    def __init__(self, ctx, src, sort=None):
        assert isinstance(src, ShuffleWritingDataset)
        super().__init__(ctx, src)
        self.sort = sort


    def parts(self):
        return [
            ShuffleReadingPartition(self, i)
            for i in range(self.src.pcount)
        ]



def prefetch(func, args_list):
    args_list = iter(args_list)
    pending = func(*next(args_list))
    for args in args_list:
        nxt = func(*args)
        yield pending
        pending = nxt
    yield pending



class ShuffleReadingPartition(Partition):
    def _materialize(self, ctx):
        sources = [
            w for w in self.dset.ctx.workers
            if w.name in self.dset.workers
        ]

        logger.debug('starting shuffle read from %r+1 sources', len(sources))

        assert len(sources) + 1 == len(self.dset.workers)

        dset_id = self.dset.src.id
        part_idx = self.idx

        # issue requests for the bucket sizes
        size_requests = [worker.get_bucket_size(dset_id, part_idx) for worker in sources]

        # make it seem like fetching locally is remote
        # so it fits in the stream_batch loop
        local = self.dset.ctx.node
        def get_local_block(*args):
            fut = Future()
            try:
                fut.set_result(local.get_bucket_block(local, *args))
            except Exception as e:
                fut.set_exception(e)
            return fut
        # add the local fetch operations
        sizes = [(get_local_block, local.get_bucket_size(local, dset_id, part_idx))]

        # wait for responses and zip with a function to get a block
        sizes += list((worker.get_bucket_block, future.result())
                     for worker, future in zip(sources, size_requests))

        streams = []
        for get_block, batches in sizes:
            for batch_idx, num_blocks in enumerate(batches):
                if num_blocks > 0:
                    blocks = [
                        (dset_id, part_idx, batch_idx, block_idx) for
                        block_idx in range(num_blocks)]
                    streams.append(prefetch(get_block, blocks))

        def block_stream_iterator(stream):
            for request in stream:
                block = request.result()
                data = block.read()
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('received shuffle block of %.2f MB, %r items', block.size / 1024 / 1024, len(data))
                del block
                yield from data

        streams = [block_stream_iterator(stream) for stream in streams]

        if self.dset.sort or self.dset.src.bucket.sorted:
            logger.debug('performing sorted shuffle read')
            yield from merge_sorted(*streams, key=self.dset.src.key)
        else:
            logger.debug('performing unsorted shuffle read')
            for stream in streams:
                yield from stream
