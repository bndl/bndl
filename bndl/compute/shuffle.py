from bisect import bisect_left
from concurrent.futures import Future, TimeoutError
from functools import lru_cache
from itertools import chain
from math import ceil
from statistics import mean
import gc
import logging
import os
import threading

from bndl.compute.dataset import Dataset, Partition
from bndl.compute.storage import StorageContainerFactory
from bndl.util.collection import batch as batch_data, ensure_collection
from bndl.util.conf import Float
from bndl.util.exceptions import catch
from bndl.util.funcs import prefetch
from bndl.util.hash import portable_hash
from bndl.util.psutil import memory_percent, used_more_pct_than, virtual_memory
from cytoolz.itertoolz import merge_sorted


logger = logging.getLogger(__name__)


max_mem_pct = Float(50)
block_size_mb = Float(4)


class Bucket:
    '''
    Base bucket class. Implements spilling to disk / serializing (spilling) to memory. Any
    implementation must implement add(element) and extend(sequence). The sorted property
    indicates whether the elements in the buckets are considered sorted (and thus elements
    from different buckets should be merge sorted in the shuffle read).
    '''

    sorted = False

    def __init__(self, bucket_id, key, comb, block_size_mb, memory_container, disk_container):
        self.id = bucket_id
        self.key = key
        self.comb = comb
        self.block_size_mb = block_size_mb
        self.memory_container = memory_container
        self.disk_container = disk_container

        self.memory_blocks = []
        self.disk_blocks = []
        self.batches = []
        self.lock = threading.RLock()
        self.element_size = None


    @property
    def memory_size(self):
        '''
        The memory footprint of a bucket in bytes. This includes elements in the bucket and the
        size of serialized in memory blocks.
        '''
        with self.lock:
            return len(self) * (self.element_size or 1) + sum(block.size for block
                                                             in self.memory_blocks)


    def _memory_blocks_todisk(self, max_mem_pct=None):
        '''
        Spill the serialized in memory blocks to disk.
        :param max_mem_pct: optional (proc_max, total_max) tuple
        '''
        blocks_spilled = 0
        bytes_spilled = 0

        # start spilling the largest in memory block first
        self.memory_blocks.sort(key=lambda block: block.size, reverse=True)
        while self.memory_blocks:
            block = self.memory_blocks.pop()
            blocks_spilled += 1
            bytes_spilled += block.size

            self.disk_blocks.append(block.to_disk())
            block = None
            gc.collect(0)

            # stop when enough was spilled
            if max_mem_pct and not used_more_pct_than(*max_mem_pct):
                break

        logger.debug('spilled %s blocks of bucket %s with %.2f mb', blocks_spilled,
                     '.'.join(map(str, self.id)), bytes_spilled / 1024 / 1024)

        return bytes_spilled


    def _estimate_element_size(self, data):
        '''
        Estimate the size of a element from data.
        '''
        test_set = data[:max(3, len(data) // 100)]
        c = self.memory_container(('tmp',))
        c.write(test_set)
        return c.size // len(test_set)


    def _spill_bucket(self, disk, max_mem_pct=None):
        '''
        Spill the elements in this bucket (not in the memory/disk blocks) to disk. The elements are
        spilled into a new batch of one ore more blocks (approximately) at most self.block_size_mb
        large.
        :param disk: bool
            Spill to disk or serialize in memory.
        :param max_mem_pct: optional (proc_max, total_max) tuple
        '''
        # create a new batch.
        batch = []
        self.batches.append(batch)
        loc_append = (self.disk_blocks if disk else self.memory_blocks).append

        # apply combiner if any
        data = ensure_collection(self.comb(self)) if self.comb else list(self)
        # and clear self
        self.clear()
        gc.collect()

        # calculate the size of an element for splitting the batch into several blocks
        if self.element_size is None:
            self.element_size = self._estimate_element_size(data)

        # split into several blocks if necessary
        block_size_recs = ceil(self.block_size_mb * 1024 * 1024 / self.element_size)
        if block_size_recs > len(data):
            blocks = [data]
        else:
            blocks = list(batch_data(data, block_size_recs))
            del data

        bytes_spilled = 0
        elements_spilled = 0
        blocks_spilled = 0

        batch_no = len(self.batches)
        container = self.disk_container if disk else self.memory_container

        while blocks:
            # write the block out to disk
            block = blocks.pop()
            c = container(self.id + ('%r.%r' % (batch_no, len(batch)),))
            c.write(block)
            batch.append(c)
            loc_append(c)

            # keep stats
            blocks_spilled += 1
            elements_spilled += len(block)
            bytes_spilled += c.size

            # trigger memory release
            block = None
            gc.collect(0)

            # when 'spilling' (serializing) to in memory blocks, and to much memory is used, then
            # move previously serialized blocks to disk and switch to spilling to disk for the
            # remainder of blocks
            if not disk and max_mem_pct and used_more_pct_than(*max_mem_pct):
                self._memory_blocks_todisk(max_mem_pct)
                disk = True

        # recalculate the size of the elements given the bytes and elements just spilled
        # the re-estimation is dampened in a decay like fashion
        self.element_size = (self.element_size + bytes_spilled // elements_spilled) // 2

        logger.debug('%s bucket %s of %.2f mb and %s recs to %s blocks @ %s b/rec',
            'spilled' if disk else 'serialized', '.'.join(map(str, self.id)),
            bytes_spilled / 1024 / 1024, elements_spilled, blocks_spilled, self.element_size)

        return bytes_spilled


    def spill(self, disk=True, max_mem_pct=None):
        with self.lock:
            bytes_spilled = 0

            # move serialized blocks in memory to disk first
            # (if spilling to disk)
            if disk and self.memory_blocks and (not max_mem_pct or used_more_pct_than(*max_mem_pct)):
                bytes_spilled += self._memory_blocks_todisk(max_mem_pct)

            # spill/serialize elements in the bucket, if any
            if len(self):
                bytes_spilled += self._spill_bucket(disk, max_mem_pct)

        return bytes_spilled



class SetBucket(Bucket, set):
    '''
    Bucket for unique elements.
    '''
    extend = set.update



class DictBucket(Bucket, dict):
    '''
    Bucket for unique elements by key
    '''
    def add(self, value):
        self[self.key(value)] = value

    def extend(self, values):
        dict.update(self, ((self.key(value), value) for value in values))

    def __iter__(self):
        return iter(self.values())



class ListBucket(Bucket, list):
    '''
    Plain bucket of elements.
    '''
    add = list.append



class SortedBucket(ListBucket):
    '''
    List bucket which is sorted before spilling and iteration.
    '''
    sorted = True

    def __iter__(self):
        self.sort()
        return list.__iter__(self)

    def spill(self, *args, **kwargs):
        self.sort()
        return super().spill(*args, **kwargs)

    def sort(self):
        return super().sort(key=self.key)



class RangePartitioner():
    '''
    A partitioner which puts elements in a bucket based on a (binary) search for a position
    in the given (sorted) boundaries.
    '''
    def __init__(self, boundaries, reverse=False):
        self.boundaries = boundaries
        self.reverse = reverse

    def __call__(self, value):
        boundaries = self.boundaries
        boundary = bisect_left(self.boundaries, value)
        return len(boundaries) - boundary if self.reverse else boundary



class ShuffleWritingDataset(Dataset):
    '''
    The writing end of a shuffle.
    '''

    # indicate that this data set must be the last one computed in a stage
    sync_required = True


    def __init__(self, ctx, src, pcount, partitioner=None, bucket=None, key=None, comb=None,
            max_mem_pct=None, block_size_mb=None, serialization='pickle',
            compression=None):
        super().__init__(ctx, src)
        self.pcount = pcount or ctx.default_pcount
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = bucket or SortedBucket
        self.key = key

        if not max_mem_pct:
            max_mem_pct = ctx.conf['bndl.compute.shuffle.max_mem_pct']
        self.max_mem_pct = (max_mem_pct / os.cpu_count(),
                            min(80, ceil(max_mem_pct + virtual_memory().percent + 10)))
        self.block_size_mb = block_size_mb or ctx.conf['bndl.compute.shuffle.block_size_mb']
        self.serialization = serialization
        self.compression = compression


    @property
    def cleanup(self):
        def _cleanup(job):
            futures = [worker.clear_bucket.with_timeout(60)(self.id) for worker in job.ctx.workers]
            for future in futures:
                try:
                    future.result()
                except TimeoutError:
                    pass
                except Exception:
                    logger.warning('unable to cleanup after job for shuffle writing dataset %s', self.id, exc_info=True)
        return _cleanup


    def parts(self):
        return [
            ShuffleWritingPartition(self, i, p)
            for i, p in enumerate(self.src.parts())
        ]


    @property
    @lru_cache()
    def disk_container(self):
        return StorageContainerFactory('disk', self.serialization, self.compression)


    @property
    @lru_cache()
    def memory_container(self):
        return StorageContainerFactory('memory', self.serialization, self.compression)



class ShuffleWritingPartition(Partition):
    def _materialize(self, ctx):
        # prepare the buckets
        buckets = ctx.node.create_buckets(self)
        bucketadd = [bucket.add for bucket in buckets]
        bucket_count = len(buckets)

        # get / create the partitioner function possibly using a key function
        key = self.dset.key
        if key:
            partitioner = self.dset.partitioner
            def part_(element):
                return partitioner(key(element))
        else:
            part_ = self.dset.partitioner

        # the relative amount of memory used to consider as ceiling
        # spill when used_more_pct_than(*max_mem_pct)
        max_mem_pct = self.dset.max_mem_pct

        # check every once in a while, every 1+/-.1 percent of allowed memory
        check_interval_min_mem = max_mem_pct[1] / 90
        check_interval_max_mem = max_mem_pct[1] / 110
        check_interval = 1024  # because we don't know on forehand how memory usage is growing

        mem_usage = memory_percent()
        check_loop = 0
        bytes_spilled = 0

        for element in self.src.materialize(ctx):
            # add each element to the bucket assigned to by the partitioner (limited by the bucket count and wrapped around)
            bucketadd[part_(element) % bucket_count](element)
            check_loop += 1

            if check_loop == check_interval:
                check_loop = 0
                mem_usage_prev = mem_usage
                mem_usage = memory_percent()

                # spill if to much memory is used
                if used_more_pct_than(*max_mem_pct):
                    bytes_spilled += ctx.node.spill_buckets(max_mem_pct)
                    logger.debug('memory usage from %r%% down to %r%% after spilling',
                                 mem_usage, memory_percent())

                # try to keep checking memory consumption at an interval related to the rate at
                # which memory is consumed
                dt_usage = mem_usage - mem_usage_prev
                if dt_usage < check_interval_min_mem:
                    check_interval <<= 1
                elif dt_usage > check_interval_max_mem:
                    check_interval = max(1, check_interval >> 1)

        logger.info('partitioned %s part %s, spilled %.2f mb',
                    self.dset.id, self.idx, bytes_spilled / 1024 / 1024)



class ShuffleReadingDataset(Dataset):
    '''
    The reading side of a shuffle.
    '''
    def __init__(self, ctx, src, sort=None):
        assert isinstance(src, ShuffleWritingDataset)
        super().__init__(ctx, src)
        self.sort = sort


    def parts(self):
        return [
            ShuffleReadingPartition(self, i)
            for i in range(self.src.pcount)
        ]



class ShuffleReadingPartition(Partition):
    def get_sources(self):
        ctx = self.dset.ctx

        # sort the source worker names relative to the name of the local worker
        # if every node does this, load will be spread more evenly in reading blocks
        source_names = sorted(self.dset.workers)
        source_names.remove(ctx.node.name)
        split = bisect_left(source_names, ctx.node.name)
        source_names = source_names[split:] + source_names[:split]

        # get the peer objects corresponding to the worker name list in self.dset
        sources = list(filter(None, (ctx.node.peers.get(w) for
                    w in source_names)))

        # and check if their all there (apart from self)
        missing = len(self.dset.workers) - (len(sources) + (1 if ctx.node.name in self.dset.workers else 0))
        assert not missing, 'missing %s shuffle sources: %s' % (missing, [name for name in source_names
                                                                          if name not in ctx.node.peers])

        return sources


    def blocks(self, max_mem_pct):
        sources = self.get_sources()
        ctx = self.dset.ctx

        # issue requests for the bucket bucket prep and get back sizes
        size_requests = [worker.finalize_bucket(self.dset.src.id, self.idx, max_mem_pct)
                         for worker in sources]

        # make it seem like fetching locally is remote
        # so it fits in the stream_batch loop
        def get_local_block(*args):
            fut = Future()
            try:
                fut.set_result(ctx.node.get_bucket_block(ctx.node, *args))
            except Exception as e:
                fut.set_exception(e)
            return fut

        # add the local fetch operations if the local node is a source
        if ctx.node.name in self.dset.workers:
            sizes = [(get_local_block,
                      ctx.node.finalize_bucket(ctx.node, self.dset.src.id, self.idx, max_mem_pct))]
        else:
            sizes = []

        # wait for responses and zip with a function to get a block
        for worker, future in zip(sources, size_requests):
            try:
                size = future.result()
            except Exception:
                logger.exception('Unable to finalize bucket %s.%s on %s' %
                                 (self.dset.src.id, self.idx, worker.name))
                raise
            sizes.append((worker.get_bucket_block, size))

        # print some workload info
        if logger.isEnabledFor(logging.INFO):
            batch_count = []
            block_count = []
            for _, batches in sizes:
                batch_count.append(len(batches))
                block_count.append(sum(batches))
            logger.info('shuffling %s batches (%s blocks) from %s workers',
                        sum(batch_count), sum(block_count), len(batch_count))
            logger.debug('batch count per source: min: %s, mean: %s, max: %s',
                         min(batch_count), mean(batch_count), max(batch_count))
            logger.debug('block count per source: min: %s, mean: %s, max: %s',
                         min(block_count), mean(block_count), max(block_count))

        # fetch batches and blocks
        for get_block, batches in sizes:
            for batch_idx, num_blocks in enumerate(batches):
                if num_blocks > 0:
                    blocks = [(self.dset.src.id, self.idx, batch_idx, block_idx)
                              for block_idx in range(num_blocks)]
                    for request in prefetch(get_block, blocks):
                        try:
                            block = request.result()
                        except Exception:
                            logger.exception('unable to retrieve block')
                            raise
                        data = block.read()
                        logger.debug('received block of %.2f mb, %r items', block.size / 1024 / 1024, len(data))
                        block = None
                        yield data
                        data = None
                        gc.collect(0)


    def _materialize(self, ctx):
        sort = self.dset.sort or self.dset.src.bucket.sorted
        worker = ctx.node

        logger.info('starting %s shuffle read', 'sorted' if sort else 'unsorted')

        # the relative amount of memory used to consider as ceiling
        # spill when used_more_pct_than(*max_mem_pct)
        max_mem_pct = self.dset.src.max_mem_pct

        # create a stream of blocks
        blocks = self.blocks(max_mem_pct)

        if sort:
            bucket = self.dset.src.bucket((self.dset.id, self.idx),
                                          self.dset.src.key, None,
                                          self.dset.src.block_size_mb,
                                          self.dset.src.memory_container,
                                          self.dset.src.disk_container)
            for block in blocks:
                bucket.extend(block)
                block = None
                worker.spill_buckets(max_mem_pct)
                if used_more_pct_than(*max_mem_pct):
                    bucket.spill(True)

            if not bucket.batches:
                return iter(bucket)
            else:
                streams = [chain.from_iterable(block.read() for block in batch)
                           for batch in bucket.batches] + [bucket]
                return merge_sorted(*streams, key=self.dset.src.key)
        else:
            return chain.from_iterable(blocks)


class ShuffleManager(object):
    def __init__(self):
        self.buckets = {}


    def create_buckets(self, part):
        '''
        Create the buckets for the data set of the given partition.
        :param part:
        '''
        dset = part.dset
        buckets = self.buckets.get(dset.id)
        if not buckets:
            mem_container = dset.memory_container
            disk_container = dset.disk_container
            buckets = [dset.bucket((dset.id, idx), dset.key, dset.comb,
                                    dset.block_size_mb, mem_container, disk_container)
                       for idx in range(dset.pcount)]
            self.buckets[dset.id] = buckets
        return buckets


    def _get_bucket(self, dset_id, bucket_idx):
        buckets = self.buckets.get(dset_id)
        if not buckets:
            msg = 'No buckets for dataset %r' % dset_id
            logger.error(msg)
            raise KeyError(msg)
        else:
            try:
                return buckets[bucket_idx]
            except IndexError:
                msg = 'No bucket %r for dataset %r' % (bucket_idx, dset_id)
                logger.exception(msg)
                raise KeyError(msg)


    def spill_buckets(self, max_mem_pct):
        spilled = 0
        if used_more_pct_than(*max_mem_pct):
            for bucket in sorted(chain.from_iterable(self.buckets.values()), key=lambda b: b.memory_size, reverse=True):
                if bucket.memory_size == 0:
                    break
                spilled += bucket.spill(True)
                if not used_more_pct_than(*max_mem_pct):
                    break
        return spilled


    def finalize_bucket(self, src, dset_id, bucket_idx, max_mem_pct):
        '''
        Prepare a bucket for shuffle read.
        
        :param src: The (rmi) peer node requesting the finalization.
        :param dset_id: The data set id.
        :param bucket_idx: The bucket id.
        :param max_mem_pct:
        '''
        bucket = self._get_bucket(dset_id, bucket_idx)
        # take the bucket lock to ensure it isn't finalized multiple times
        with bucket.lock:
            spilled = bucket.spill(False, max_mem_pct)
            logger.debug('prepared bucket %s.%s for shuffle read, spilled %.2f mb',
                         dset_id, bucket_idx, spilled / 1024 / 1024)
            return [len(batch) for batch in bucket.batches]


    def get_bucket_block(self, src, dset_id, bucket_idx, batch_idx, block_idx):
        '''
        Retrieve a block from a batch in a bucket.
        
        :param src: The (rmi) peer node requesting the block.
        :param dset_id: The id of the data set.
        :param bucket_idx: The bucket index.
        :param batch_idx: The batch index.
        :param block_idx: The block index.
        '''
        bucket = self._get_bucket(dset_id, bucket_idx)

        # get the block while holding the bucket lock to ensure it isn't spilled at the same time
        with bucket.lock:
            try:
                batch = bucket.batches[batch_idx]
            except IndexError:
                msg = 'No batch %r in partition %r of dataset %r' % (
                    batch_idx, bucket_idx, dset_id)
                logger.exception(msg)
                raise KeyError(msg)

            try:
                return batch[block_idx]
            except IndexError:
                msg = 'No block %r in batch %r of partition %r in dataset %r' % (
                    block_idx, batch_idx, bucket_idx, dset_id)
                logger.exception(msg)
                raise KeyError(msg)


    def clear_bucket(self, src, dset_id, part_idx=None):
        try:
            if part_idx is not None:
                buckets = self.buckets.get(dset_id)
                buckets[part_idx].clear()
                del buckets[part_idx]
            else:
                for bucket in self.buckets.get(dset_id, ()):
                    bucket.clear()
                with catch(KeyError):
                    del self.buckets[dset_id]
            gc.collect()
        except KeyError:
            pass
