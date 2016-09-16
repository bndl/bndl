from __future__ import print_function

from bisect import bisect_left
from concurrent.futures._base import Future
from itertools import cycle, islice
import gc
import logging
import time

from bndl.compute.dataset import Dataset, Partition
from bndl.compute.storage import StorageContainerFactory
from bndl.util.collection import batch as batch_data, ensure_collection
from bndl.util.hash import portable_hash
from cytoolz.itertoolz import merge_sorted
from psutil import virtual_memory
from toolz.itertoolz import interleave


logger = logging.getLogger(__name__)


BLOCK_FRACTION = 8


class Bucket:
    sorted = False

    def __init__(self, idx, key, comb, min_block_size, default_container):
        self.idx = idx
        self.key = key
        self.comb = comb
        self.min_block_size = min_block_size
        self.default_container = default_container
        self.batches = []


    def spill(self, container=None):
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
        data = ensure_collection(self.comb(self)) if self.comb else self
        idx = self.idx

        # write out data in blocks
        block_count = max(len(data) // BLOCK_FRACTION, self.min_block_size)
        blocks = batch_data(data, block_count)
        for block in blocks:
            c = container(idx + (batch_no, len(batch)))
            c.write(block)
            add_block(c)

        #print('batch', len(self.batches) - 1, 'chunked', len(data), 'records in', len(batch), 'chunks')

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
        boundary = bisect_left(boundaries, value)
        return len(boundaries) - boundary if self.reverse else boundary



class ShuffleWritingDataset(Dataset):
    def __init__(self, ctx, src, pcount, partitioner=None, bucket=None, key=None, comb=None,
            max_mem_percentage=80,
            check_interval_min_mem=-100 * 1000 * 1000,
            check_interval_max_mem=-500 * 1000 * 1000,
            check_interval=1000,
            min_block_size=1000,
            serialization='pickle',
            compression=None):
        super().__init__(ctx, src)
        self.pcount = pcount or len(self.src.parts())
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = bucket or SortedBucket
        self.key = key

        self.max_mem_percentage = max_mem_percentage
        self.check_interval_min_mem = check_interval_min_mem
        self.check_interval_max_mem = check_interval_max_mem
        self.check_interval = check_interval
        self.min_block_size = min_block_size
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
                                        self.dset.min_block_size, self._memory_container)
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

        cdef int check_loop = 0
        cdef int check_interval = 0
#         check_loop = 0

        max_mem_percentage = self.dset.max_mem_percentage
        check_interval_min_mem = self.dset.check_interval_min_mem
        check_interval_max_mem = self.dset.check_interval_max_mem
        check_interval = self.dset.check_interval

        vmem = virtual_memory()

        for element in self.src.materialize(ctx):
            bucketadd[part_(element) % bucket_count](element)

            check_loop += 1
            if check_loop == check_interval:
                check_loop = 0
                vmem_prev = vmem
                vmem = virtual_memory()
                if vmem.percent > max_mem_percentage:
                # for testing, TODO: remove
#                 if vmem.percent > max_mem_percentage or True:
                    for bucket in sorted(buckets, key=len, reverse=True):
                        print('flushed', id(bucket), 'of len', len(bucket), '...', virtual_memory().percent, '% memory used')
                        bucket.spill(self._disk_container)
                        gc.collect()
                        time.sleep(.1)
                        # TODO
                        if virtual_memory().percent < max_mem_percentage:
                            print('flushed enough')
                            break
#                     for bucket in sorted(buckets, key=len, reverse=True):
#                         bucket.spill(self._disk_container)
#                         gc.collect()
#                         # TODO
#                         time.sleep(.1)
#                         print('flushed', id(bucket), virtual_memory().percent, '% memory used')
#                         if virtual_memory().percent < max_mem_percentage:
#                             break
                dt_available = vmem.available - vmem_prev.available
                if dt_available >= check_interval_min_mem:
                    check_interval <<= 1
                elif dt_available <= check_interval_max_mem:
                    check_interval = max(1, check_interval >> 1)
                print('available memory', vmem.percent, '%', '(max', max_mem_percentage, ')')
                # print('memory increase', -(round(dt_available/1024/1024, 2)), 'mb', 'check every', check_interval, 'loops')



class ShuffleReadingDataset(Dataset):
    def __init__(self, ctx, src):
        super().__init__(ctx, src)
        assert isinstance(src, ShuffleWritingDataset)


    def parts(self):
        return [
            ShuffleReadingPartition(self, i)
            for i in range(self.src.pcount)
        ]



class ShuffleReadingPartition(Partition):
    def _materialize(self, ctx):
        workers = self.dset.ctx.workers
        dset_id = self.dset.src.id
        part_idx = self.idx

        # issue requests for the bucket sizes
        sizes = [worker.get_bucket_size(dset_id, part_idx) for worker in workers]
        # wait for responses and zip with a function to get a block
        sizes = list((worker.get_bucket_block, future.result())
                     for worker, future in zip(workers, sizes))

        # make it seem like fetching locally is remote
        # so it fits in the stream_batch loop
        local = self.dset.ctx.node
        def _get_local_block(*args):
            fut = Future()
            try:
                fut.set_result(local.get_bucket_block(local, *args))
            except Exception as e:
                fut.set_exception(e)
            return fut
        # add the local fetch operations
        sizes += [(_get_local_block, local.get_bucket_size(local, dset_id, part_idx))]

        def prefetch(func, args_list):
            args_list = iter(args_list)
            pending = func(*next(args_list))
            for args in args_list:
                nxt = func(*args)
                yield  pending
                pending = nxt
            yield  pending

        # stream blocks from a batch with 1 block prefetch
        def stream_batch(get_block, batch_idx, num_blocks):
            blocks = (
                (dset_id, part_idx, batch_idx, block_idx)
                for block_idx in range(num_blocks)
            )
            for block in prefetch(get_block, blocks):
                yield from block.result().read()
#                 yield block.result().read()

#             for block in blocks:
#                 yield from get_block(*block).result().read()

        # create streams across workers and the batches in their bucket
        streams = []
        for get_block, batches in sizes:
            for batch_idx, num_blocks in enumerate(batches):
                if num_blocks > 0:
                    streams.append(stream_batch(get_block, batch_idx, num_blocks))

        if self.dset.src.bucket.sorted:
            # merge the streams in a sorted fashion
            # yield from merge_sorted(*[(e  for b in stream for e in b) for stream in streams], key=key)
#             yield from merge_sorted(*[(iter(b)  for b in stream) for stream in streams], key=key)
            yield from merge_sorted(*streams, key=self.dset.src.key)
        else:
#
#             available = []
#             for stream in streams:

            # not really faster, but still fast
#             remaining = len(streams)
#             nexts = cycle(iter(it).__next__ for it in streams)
#             while remaining:
#                 try:
#                     for nxt in nexts:
#                         yield from nxt()
#                 except StopIteration:
#                     remaining -= 1
#                     nexts = cycle(islice(nexts, remaining))


            # faster still
            for x in prefetch(iter, ((s,) for s in streams)):
                yield from x
#                 for block in x:
#                     yield from block


#             streams = [stream.__next__ for stream in streams]
#             idx =0
#             while True:
#                 for stream in cycle(streams):
#                     x = stream()
#                     print(str(x))
#                     yield x
#                 for idx in range(len(streams))
#                 x = stream_loop
#                 print(x)
#                 yield from x

            # a bit faster ...
#             for stream in streams:
#                 yield from stream

            # slow ...
            # yield from interleave(streams)
