from bisect import bisect_left
from concurrent.futures import Future
from functools import lru_cache
from itertools import chain
from math import ceil
from statistics import mean, StatisticsError
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
from bndl.util.psutil import memory_percent, virtual_memory
from cytoolz.itertoolz import merge_sorted


logger = logging.getLogger(__name__)


max_mem_pct = Float(50, desc='Percentage (1-100) indicating the amount of memory to be used for shuffling.')
block_size_mb = Float(4, desc='Target (maximum) size (in megabytes) of blocks created by spilling / serializing '
                              'elements to disk')


MAX_MEM_LOW_WATER = 0.8


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

        # batches (lists) of blocks serialized / spilled
        self.batches = []
        # blocks located on memory and disk
        self.memory_blocks = []
        self.disk_blocks = []
        # lock protecting batches and {memory/disk}_blocks
        self.lock = threading.RLock()

        # estimate of the size of an element (after combining)
        self.element_size = None


    @property
    def memory_size(self):
        '''
        The memory footprint of a bucket in bytes. This includes elements in the bucket and the
        size of serialized in memory blocks.

        When this bucket hasn't been serialized / spilled yet, the memory_size is grossly
        inaccurate; but can be used to compare with other buckets with similar contents which
        haven't been serialized / spilled either.
        '''
        with self.lock:
            return len(self) * (self.element_size or 1) + sum(block.size for block
                                                             in self.memory_blocks)


    def _spill_memory_blocks(self):
        '''
        Spill (migrate) the serialized in memory blocks to disk.
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
        area_append = (self.disk_blocks if disk else self.memory_blocks).append

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

            # add references to the block in the batch and the {memory,disk}_blocks list
            batch.append(c)
            area_append(c)

            # keep stats
            blocks_spilled += 1
            elements_spilled += len(block)
            bytes_spilled += c.size

            # trigger memory release
            block = None

        # recalculate the size of the elements given the bytes and elements just spilled
        # the re-estimation is dampened in a decay like fashion
        self.element_size = (self.element_size + bytes_spilled // elements_spilled) // 2

        logger.debug('%s bucket %s of %.2f mb and %s recs to %s blocks @ %s b/rec',
            'spilled' if disk else 'serialized', '.'.join(map(str, self.id)),
            bytes_spilled / 1024 / 1024, elements_spilled, blocks_spilled, self.element_size)

        return bytes_spilled


    def serialize(self, disk=True):
        '''
        Serialize / spill the bucket to memory or disk.

        :param disk: bool
            Whether to spill the bucket to disk (True) or serialize in memory (False).
        '''
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
        self.n_boundaries = len(boundaries)
        self.reverse = reverse
        if reverse:
            self.__call__ = self.partition_reversed

    def __call__(self, value):
        return bisect_left(self.boundaries, value)

    def partition_reversed(self, value):
        boundary = bisect_left(self.boundaries, value)
        return self.n_boundaries - boundary



class ShuffleWritingDataset(Dataset):
    '''
    The writing end of a shuffle.
    '''

    # indicate that this data set must be the last one computed in a stage
    sync_required = True


    def __init__(self, src, pcount, partitioner=None, bucket=None, key=None, comb=None,
            max_mem_pct=None, block_size_mb=None, serialization='pickle',
            compression=None):
        '''
        :param src: Dataset
            Dataset to be shuffled.
        :param pcount: int or None
            The number of partitions to shuffle to, defaults to the number of source partitions.
        :param partitioner: fun(element): int or None
            The partitioner to use. Defaults to portable_hash
        :param bucket: Bucket or None
            The class of bucket to use, defaults to SortedBucket
        :param key: fun(element): obj or None
            The key function to apply to each element. The output is used to partition (and sort
            if applicable) the data on.
        :param comb: fun(sequence): iterable or None
            Optional combiner to apply on a bucket before serialization.
        :param max_mem_pct: int between 0 and 100 or None
            Amount of memory to allow for the shuffle. Defaults to
            bndl.compute.shuffle.max_mem_pct / os.cpu_count() (determined at the worker)
        :param block_size_mb: float or None
            The size of the blocks to produce in serializing the shuffle data. Defaults to
            bndl.compute.shuffle.block_size_mb.
        :param serialization: str or None
            A string compatible to the serialization parameter of StorageContainerFactory. E.g.
            'pickle', 'marshal', 'text', 'binary', 'json', etc. Defaults to 'pickle'.
        :param compression: str or None
            A string compatible to the compression parameter of StorageContainerFactory. E.g. 'gzip'.
        '''
        super().__init__(src.ctx, src)
        self.pcount = pcount or len(src.parts())
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = bucket or SortedBucket
        self.key = key

        self._max_mem_pct = max_mem_pct or src.ctx.conf['bndl.compute.shuffle.max_mem_pct']
        self.block_size_mb = block_size_mb or src.ctx.conf['bndl.compute.shuffle.block_size_mb']
        self.serialization = serialization
        self.compression = compression


    @property
    def cleanup(self):
        def _cleanup(job):
            for worker in job.ctx.workers:
                worker.clear_bucket(self.id)
        return _cleanup


    def parts(self):
        return [
            ShuffleWritingPartition(self, i, p)
            for i, p in enumerate(self.src.parts())
        ]


    @property
    def max_mem_pct(self):
        return self._max_mem_pct / os.cpu_count()


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
        # spill when memory_percent() > max_mem_pct
        max_mem_pct = self.dset.max_mem_pct

        def calc_check_interval(buckets):
            try:
                element_size = mean(bucket.element_size for bucket in buckets if bucket.element_size)
            except StatisticsError:
                # default to a rather defensive interval without going 'overboard' (i.e. interval == 1)
                return 16
            else:
                # check 10 times in the memory budget
                return int((max_mem_pct / 100) * virtual_memory().total / 10 / element_size)

        # check every once in a while
        check_interval = calc_check_interval(chain.from_iterable(ctx.node._buckets_for_dset(self.dset.id).values()))
        check_loop = 0

        # stat counters
        bytes_spilled = 0
        bytes_serialized = 0
        elements_partitioned = 0

        # add each element to the bucket assigned to by the partitioner
        for element in self.src.materialize(ctx):
            # (limited by the bucket count and wrapped around)
            bucketadd[part_(element) % bucket_count](element)
            check_loop += 1

            # check once in a while
            if check_loop > check_interval:
                # keep track of no. elements partitioned without having two counters,
                # or performing a modulo check_interval in each loop
                elements_partitioned += check_loop
                check_loop = 0

                # spill if to much memory is used
                mem_used = memory_percent()
                if mem_used > max_mem_pct:
                    # spill enough to get below the high water mark - 20%
                    spilled = ctx.node.spill_buckets(max_mem_pct * MAX_MEM_LOW_WATER)
                    bytes_spilled += spilled

                    if spilled:
                        # update the check frequency according to the size of
                        # the serialized (and possibly combined) records
                        check_interval = calc_check_interval(buckets)

                    logger.debug('memory usage from %.2f%% down to %.2f%% after spilling %.2f mb',
                                 mem_used, memory_percent(), spilled / 1024 / 1024)

                elif mem_used < max_mem_pct * MAX_MEM_LOW_WATER:
                    # check less often if enough memory available
                    check_interval *= 2

        # for efficiency elements_partitioned is tracked every check_interval
        # so after partitioning check_loop contains the number of elements
        # since the last check
        elements_partitioned += check_loop

        # serialize / spill the buckets for shuffle read
        # and keep track of the number of bytes spilled / serialized
        for bucket in buckets:
            if memory_percent() > max_mem_pct:
                bytes_spilled += bucket.serialize(True)
            else:
                bytes_serialized += bucket.serialize(False)

        logger.info('partitioned %s.%s with %s elements, spilled %.2f mb to disk, '
                    'serialized %.2f mb to memory', self.dset.id, self.idx, elements_partitioned,
                    bytes_spilled / 1024 / 1024, bytes_serialized / 1024 / 1024)



class ShuffleReadingDataset(Dataset):
    '''
    The reading side of a shuffle.

    It is expected that the source of ShuffleReadingDataset is a ShuffleWritingDataset
    (or at least compatible to it). Many properties of it are read in the shuffle read, i.e.:
    block_size_mb, max_mem_pct, key, bucket.sorted, memory_container, disk_container
    '''
    def __init__(self, src, sort=None):
        assert isinstance(src, ShuffleWritingDataset)
        super().__init__(src.ctx, src)
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


    def sizes(self):
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
                fut.set_result(ctx.node.get_bucket_blocks(ctx.node, *args))
            except Exception as e:
                fut.set_exception(e)
            return fut

        # add the local fetch operations if the local node is a source
        if ctx.node.name in self.dset.workers:
            sizes = [(ctx.node,
                      get_local_block,
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
            sizes.append((worker, worker.get_bucket_blocks, size))

        # print some workload info
        if logger.isEnabledFor(logging.INFO):
            batch_count = []
            block_count = []
            total_size = 0
            for * _, parts in sizes:
                for _, batches in parts:
                    batch_count.append(len(batches))
                    block_count.append(sum(len(block_sizes) for block_sizes in batches))
                    total_size += sum(sum(block_sizes) for block_sizes in batches)
            logger.info('shuffling %.1f mb (%s batches, %s blocks) from %s workers',
                        total_size / 1024 / 1024, sum(batch_count), sum(block_count), len(batch_count))
            logger.debug('batch count per source: min: %s, mean: %s, max: %s',
                         min(batch_count), mean(batch_count), max(batch_count))
            logger.debug('block count per source: min: %s, mean: %s, max: %s',
                         min(block_count), mean(block_count), max(block_count))

        return sizes


    def blocks(self):
        # batch fetches as 'multi-gets' to minimize the waiting on network I/O
        src_dset_id = self.dset.src.id
        dest_part_idx = self.idx
        block_size_b = self.dset.src.block_size_mb * 1024 * 1024

        for worker, get_blocks, parts in self.sizes():
            requests = []
            request = []
            request_size = 0

            def new_batch():
                nonlocal request, request_size
                if request_size:
                    requests.append((request,))
                    request = []
                    request_size = 0

            for src_part_idx, batches in parts:
                for batch_idx, block_sizes in enumerate(batches):
                    for block_idx, block_size in enumerate(block_sizes):
                        if request_size + block_size > block_size_b:
                            new_batch()
                        request.append((src_dset_id, src_part_idx, dest_part_idx, batch_idx, block_idx))
                        request_size += block_size

            new_batch()

            # perform the multi-gets for this source and apply pre-fetching / read-ahead
            for request in prefetch(get_blocks, requests):
                try:
                    blocks = request.result()
                except Exception:
                    logger.exception('unable to retrieve blocks from %s', worker.name)
                    raise
                else:
                    yield from blocks


    def _materialize(self, ctx):
        sort = self.dset.sort or self.dset.src.bucket.sorted
        worker = ctx.node

        logger.info('starting %s shuffle read', 'sorted' if sort else 'unsorted')

        # the relative amount of memory used to consider as ceiling
        # spill when memory_percent() > max_mem_pct
        max_mem_pct = self.dset.src.max_mem_pct

        # create a stream of blocks
        blocks = self.blocks()

        if sort:
            bucket = self.dset.src.bucket((self.dset.id, self.idx),
                                          self.dset.src.key, None,
                                          self.dset.src.block_size_mb,
                                          self.dset.src.memory_container,
                                          self.dset.src.disk_container)

            # check 10 times per memory budget if more memory used than available
            check_interval = (max_mem_pct / 100) * virtual_memory().total // 10
            check_val = 0
            bytes_received = 0

            for block in blocks:
                block_size = block.size
                check_val += block_size
                bytes_received += block_size

                data = block.read()
                logger.debug('received block of %.2f mb, %r items', block_size / 1024 / 1024, len(data))

                block = None
                bucket.extend(data)
                data = None

                if check_val > check_interval:
                    # spill source buckets under progressively higher pressure
                    max_mem_pct_tmp = max_mem_pct
                    while memory_percent() > max_mem_pct:
                        spilled = worker.spill_buckets(max_mem_pct_tmp)
                        # break if nothing could be spilled
                        if not spilled:
                            break
                        # lower what's left for source buckets
                        max_mem_pct_tmp //= 2

                    # spill the destination bucket if need be
                    if memory_percent() > max_mem_pct:
                        spilled = bucket.serialize(True)

            logger.debug('sorting %.1f mb', bytes_received / 1024 / 1024)

            if not bucket.batches:
                return iter(bucket)
            else:
                streams = [bucket]
                for batch in bucket.batches:
                    streams.append(chain.from_iterable(block.read() for block in reversed(batch)))
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
        '''
        Spill buckets known by this ShuffleManager (worker).

        When max_mem_pct is set, buckets are spilled in order of their (estimated) memory footprint.

        :param max_mem_pct: int between 0 and 100 or None
            Spill until memory_percent() < max_mem_pct or spill everything if max_mem_pct is None
        '''
        buckets = chain.from_iterable(
            chain.from_iterable(buckets.values())
            for buckets in self.buckets.values()
        )

        spilled = 0

        # spill only when under memory pressure (if max_mem_pct set)
        # or just spill all
        if max_mem_pct:
            if memory_percent() > max_mem_pct:
                for bucket in sorted(buckets, key=lambda b: b.memory_size, reverse=True):
                    if bucket.memory_size == 0:
                        break
                    spilled += bucket.serialize(True)
                    if memory_percent() < max_mem_pct:
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
            sizes.append((src_part_idx, [[block.size for block in batch]
                                         for batch in bucket.batches]))
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


    def get_bucket_blocks(self, src, coordinates):
        '''
        Retrieve blocks.

        :param src: requesting peer
        :param coordinates: sequence of (src_dset_id, src_part_idx, dest_part_idx, batch_idx, block_idx) tuples
        '''
        return [self.get_bucket_block(src, *coordinate)
                for coordinate in coordinates]


    def clear_bucket(self, src, dset_id):
        '''
        Clear all the buckets for a dataset
        :param src: requesting peer
        :param dset_id: Id of the dataset to clear buckets for.
        '''
        with catch(KeyError):
            del self.buckets[dset_id]
            gc.collect()
