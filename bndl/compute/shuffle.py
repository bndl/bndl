from bisect import bisect_left
from concurrent.futures import Future, TimeoutError
from functools import lru_cache
from itertools import chain
from math import ceil, floor
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


    def _spill_memory_blocks(self):
        '''
        Spill the serialized in memory blocks to disk.
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


    def _serialize_bucket(self, disk):
        '''
        Serialize the elements in this bucket (not in the memory/disk blocks) to disk or in blocks
        in memory. The elements are serialized into a new batch of one ore more blocks
        (approximately) at most self.block_size_mb large.
        :param disk: bool
            Serialize to disk or memory.
        '''
        # create a new batch.
        batch = []
        self.batches.append(batch)
        loc_append = (self.disk_blocks if disk else self.memory_blocks).append

        # apply combiner if any
        data = ensure_collection(self.comb(self)) if self.comb else list(self)
        # and clear self
        self.clear()

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

        gc.collect()

        # recalculate the size of the elements given the bytes and elements just spilled
        # the re-estimation is dampened in a decay like fashion
        self.element_size = (self.element_size + bytes_spilled // elements_spilled) // 2

        logger.debug('%s bucket %s of %.2f mb and %s recs to %s blocks @ %s b/rec',
            'spilled' if disk else 'serialized', '.'.join(map(str, self.id)),
            bytes_spilled / 1024 / 1024, elements_spilled, blocks_spilled, self.element_size)

        return bytes_spilled


    def serialize(self, disk=True):
        with self.lock:
            bytes_serialized = 0

            # move serialized blocks in memory to disk first
            # (if spilling to disk)
            if disk and self.memory_blocks:
                bytes_serialized += self._spill_memory_blocks()

            # spill/serialize elements in the bucket, if any
            if len(self):
                bytes_serialized += self._serialize_bucket(disk)

        return bytes_serialized



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

    def _serialize_bucket(self, disk):
        self.sort()
        return super()._serialize_bucket(disk)

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
        check_interval_min_mem = max_mem_pct[1] / 100 * 0.9
        check_interval_max_mem = max_mem_pct[1] / 100 * 1.1
        check_interval = 1024  # because we don't know on forehand how memory usage is growing

        check_loop = 0

        bytes_spilled = 0
        bytes_serialized = 0
        elements_partitioned = 0

        for element in self.src.materialize(ctx):
            # add each element to the bucket assigned to by the partitioner (limited by the bucket count and wrapped around)
            bucketadd[part_(element) % bucket_count](element)
            check_loop += 1

            if check_loop == check_interval:
                # keep track of no. elements partitioned without having two counters,
                # or performing a modulo check_interval in each loop
                elements_partitioned += check_loop
                check_loop = 0

                # spill if to much memory is used
                if used_more_pct_than(*max_mem_pct):
                    mem_usage = memory_percent()
                    # spill enough to get below the high water mark
                    bytes_spilled += ctx.node.spill_buckets(max_mem_pct)
                    logger.debug('memory usage from %r%% down to %r%% after spilling',
                                 mem_usage, memory_percent())
                    check_interval = max(1, check_interval // 2)
                else:
                    check_interval = floor(check_interval * 1.5)

        # for efficiency elements_partitioned is tracked every check_interval
        # so after partitioning check_loop contains the number of elements
        # since the last check
        elements_partitioned += check_loop

        # serialize / spill the buckets for shuffle read
        # and keep track of the number of bytes spilled / serialized
        for bucket in buckets:
            if used_more_pct_than(*max_mem_pct):
                bytes_spilled += bucket.serialize(True)
            else:
                bytes_serialized += bucket.serialize(False)

        logger.info('partitioned %s.%s with %s elements, spilled %.2f mb to disk, '
                    'serialized %.2f mb to memory', self.dset.id, self.idx, elements_partitioned,
                    bytes_spilled / 1024 / 1024, bytes_serialized / 1024 / 1024)



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


    def blocks(self):
        sources = self.get_sources()
        ctx = self.dset.ctx

        # issue requests for the bucket bucket prep and get back sizes
        size_requests = [worker.get_bucket_sizes(self.dset.src.id, self.idx)
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
                      ctx.node.get_bucket_sizes(ctx.node, self.dset.src.id, self.idx))]
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
            for _, parts in sizes:
                for _, batches in parts:
                    batch_count.append(len(batches))
                    block_count.append(sum(batches))
            logger.info('shuffling %s batches (%s blocks) from %s workers',
                        sum(batch_count), sum(block_count), len(batch_count))
            logger.debug('batch count per source: min: %s, mean: %s, max: %s',
                         min(batch_count), mean(batch_count), max(batch_count))
            logger.debug('block count per source: min: %s, mean: %s, max: %s',
                         min(block_count), mean(block_count), max(block_count))

        # fetch batches and blocks
        for get_block, parts in sizes:
            for src_part_idx, batches in parts:
                for batch_idx, num_blocks in enumerate(batches):
                    if num_blocks > 0:
                        blocks = [(self.dset.src.id, src_part_idx, self.idx, batch_idx, block_idx)
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
        blocks = self.blocks()

        if sort:
            bucket = self.dset.src.bucket((self.dset.id, self.idx),
                                          self.dset.src.key, None,
                                          self.dset.src.block_size_mb,
                                          self.dset.src.memory_container,
                                          self.dset.src.disk_container)
            for block in blocks:
                bucket.extend(block)
                block = None
                if used_more_pct_than(*max_mem_pct):
                    # will cause every source bucket to be spilled
                    worker.spill_buckets()
                if used_more_pct_than(*max_mem_pct):
                    # spill the destination bucket if need be
                    bucket.serialize(True)

            if not bucket.batches:
                return iter(bucket)
            else:
                streams = [chain.from_iterable(block.read() for block in reversed(batch))
                           for batch in bucket.batches] + [bucket]
                return merge_sorted(*streams, key=self.dset.src.key)
        else:
            return chain.from_iterable(blocks)



class ShuffleManager(object):
    def __init__(self):
        # output buckets are indexed by (nested dicts)
        # - shuffle write data set id
        # - source part index
        # - destination part index
        self.buckets = {}
        self.buckets_lock = threading.Lock()


    def create_buckets(self, part):
        '''
        Create the buckets for the data set of the given partition.
        :param part: The partition of the source (shuffle writing) data set.
        '''
        with self.buckets_lock:
            dset = part.dset
            # ensure that nothing is overwritten and ensure there is a dset level container
            assert part.idx not in self.buckets.setdefault(dset.id, {})
            # create a bucket for each output partition
            buckets = [dset.bucket((dset.id, part.idx, idx), dset.key, dset.comb,
                                    dset.block_size_mb, dset.memory_container, dset.disk_container)
                       for idx in range(dset.pcount)]
            self.buckets[dset.id][part.idx] = buckets
        return buckets


    def spill_buckets(self, max_mem_pct=None):
        buckets = chain.from_iterable(
            chain.from_iterable(buckets.values())
            for buckets in self.buckets.values()
        )

        spilled = 0

        # spill only when under memory presure (if max_mem_pct set)
        # or just spill all
        if max_mem_pct:
            if used_more_pct_than(*max_mem_pct):
                for bucket in sorted(buckets, key=lambda b: b.memory_size, reverse=True):
                    if bucket.memory_size == 0:
                        break
                    spilled += bucket.serialize(True)
                    if not used_more_pct_than(*max_mem_pct):
                        break
        else:
            for bucket in buckets:
                spilled += bucket.serialize(True)
        return spilled


    def _buckets_for_dset(self, src_dset_id):
        buckets = self.buckets[src_dset_id]
        if not buckets:
            msg = 'No buckets for dataset %r' % src_dset_id
            logger.error(msg)
            raise KeyError(msg)
        return buckets


    def get_bucket_sizes(self, src, src_dset_id, dest_part_idx):
        '''
        Return the sizes and coordinates of the buckets for the destination partition.
        
        :param src: The (rmi) peer node requesting the finalization.
        :param dset_id: The id of the source data set.
        :param dest_part_idx: The index of the destination partition.
        '''
        sizes = []
        for src_part_idx, buckets in self._buckets_for_dset(src_dset_id).items():
            bucket = buckets[dest_part_idx]
            sizes.append((src_part_idx, [len(batch) for batch in bucket.batches]))
        return sizes


    def get_bucket_block(self, src, src_dset_id, src_part_idx, dest_part_idx, batch_idx, block_idx):
        '''
        Retrieve a block from a batch in a bucket.
        '''
        buckets = self._buckets_for_dset(src_dset_id)
        buckets = buckets.get(src_part_idx)
        if not buckets:
            msg = 'No buckets for source partition %s in dataset %s' % (src_part_idx, src_dset_id)
            logger.error(msg)
            raise KeyError(msg)

        try:
            bucket = buckets[dest_part_idx]
        except IndexError:
            msg = 'No destination bucket %s in partition %s of dataset %s' % \
                  (dest_part_idx, src_part_idx, src_dset_id)
            logger.exception(msg)
            raise KeyError(msg)

        # get the block while holding the bucket lock to ensure it isn't spilled at the same time
        with bucket.lock:
            try:
                batch = bucket.batches[batch_idx]
            except IndexError:
                msg = 'No batch %s in destination bucket %s in partition %s of dataset %s' % \
                      (batch_idx, dest_part_idx, src_part_idx, src_dset_id)
                logger.exception(msg)
                raise KeyError(msg)

            try:
                return batch[block_idx]
            except IndexError:
                msg = 'No block %s in batch %s in destination bucket %s in partition %s of dataset %s' % \
                      (block_idx, batch_idx, dest_part_idx, src_part_idx, src_dset_id)
                logger.exception(msg)
                raise KeyError(msg)


    def clear_bucket(self, src, dset_id):
        with catch(KeyError):
            del self.buckets[dset_id]
            gc.collect()
