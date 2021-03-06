# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bisect import bisect_left
from functools import lru_cache
from itertools import chain
from math import ceil
from operator import attrgetter
from statistics import mean
import gc
import logging
import os

from cytoolz.itertoolz import pluck

from bndl.compute.dataset import Dataset, Partition
from bndl.compute.scheduler import DependenciesFailed
from bndl.compute.storage import ContainerFactory
from bndl.compute.tasks import TaskCancelled
from bndl.compute.tasks import task_context
from bndl.net import rmi
from bndl.net.connection import NotConnected
from bndl.net.rmi import InvocationException
from bndl.util.collection import batch as batch_data, ensure_collection, flatten, sortgroupby
from bndl.util.conf import Float
from bndl.util.funcs import star_prefetch, identity, getter
from bndl.util.hash import portable_hash
from cyheapq import merge


PARTITIONED_SORT_SPLITS = 128


logger = logging.getLogger(__name__)


block_size_mb_net = Float(8, desc='Target (maximum) size (in megabytes) of blocks created in the '
                                  'shuffle write (during spill and for transmission to shuffle '
                                  'read).')
block_size_mb_merge = Float(100, desc='Target (maximum) size (in megabytes) of blocks created by '
                                      'spilling for merge sort in shuffle read. Determines size '
                                      'of files to merge.')
chunk_size_mb_merge = Float(1, desc='Target (maximum) size (in megabytes) of chunks created by '
                                    'spilling for merge sort in shuffle read. Determines size '
                                    'of reads during merge.')


class Bucket:
    '''
    Base bucket class. Implements spilling to disk / serializing (spilling) to memory. Any
    implementation must implement add(element) and extend(sequence). The sorted property
    indicates whether the elements in the buckets are considered sorted (and thus elements
    from different buckets should be merge sorted in the shuffle read).
    '''

    sorted = False

    def __init__(self, bucket_id, key, comb, block_size_mb, chunk_size_mb, memory_container, disk_container):
        self.id = bucket_id
        self.key = key
        self.comb = comb
        self.block_size_mb = block_size_mb
        self.chunk_size_mb = chunk_size_mb
        self.memory_container = memory_container
        self.disk_container = disk_container

        # batches (lists) of blocks serialized / spilled
        self.batches = []

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
        return len(self) * (self.element_size or 1)


    def _estimate_element_size(self, data):
        '''
        Estimate the size of a element from data.
        '''
        test_set = data[:max(10, len(data) // 1000)]
        c = self.memory_container(self.id + ('tmp',))
        try:
            c.write(test_set)
            return c.size // len(test_set)
        finally:
            c.clear()


    def _to_blocks(self):
        # apply combiner if any
        data = ensure_collection(self.comb(self)) if self.comb else list(self)
        # and clear self
        self.clear()

        # calculate the size of an element for splitting the batch into several blocks
        if self.element_size is None:
            self.element_size = self._estimate_element_size(data)
        mb_per_el = 1024 * 1024 / max(1, self.element_size)

        # split into several blocks and chunks if necessary
        block_size_recs = ceil(self.block_size_mb * mb_per_el)

        if block_size_recs > len(data):
            blocks = [data]
        else:
            blocks = list(batch_data(data, block_size_recs))
        if self.chunk_size_mb != self.block_size_mb:
            chunk_size_recs = ceil(self.chunk_size_mb * mb_per_el)
            for i in range(len(blocks)):
                blocks[i] = list(batch_data(blocks[i], chunk_size_recs))
            return blocks
        else:
            return [[b] for b in blocks]


    def serialize(self, disk):
        '''
        Serialize the elements in this bucket (not in the memory/disk blocks) to disk or in blocks
        in memory. The elements are serialized into a new batch of one ore more blocks
        (approximately) at most self.block_size_mb large in chunks of at most
        self.chunk_size_mb large.

        :param disk: bool
            Serialize to disk or memory.
        '''
        if not len(self):
            return 0

        # create a new batch.
        batch = []
        self.batches.append(batch)

        bytes_spilled = 0
        elements_spilled = 0
        blocks_spilled = 0
        chunks_spilled = 0
        batch_no = len(self.batches)

        Container = self.disk_container if disk else self.memory_container

        blocks = self._to_blocks()
        while blocks:
            block = blocks.pop()
            container = Container(self.id + ('%r.%r' % (batch_no, len(batch)),))
            container.write_all(block)
            batch.append(container)

            blocks_spilled += 1
            chunks_spilled += len(block)
            elements_spilled += sum(map(len, block))
            bytes_spilled += container.size

        # recalculate the size of the elements given the bytes and elements just spilled
        # the re-estimation is dampened in a decay like fashion
        self.element_size = (self.element_size + bytes_spilled // elements_spilled) // 2

        logger.debug('%s bucket %s of %.2f mb and %s recs to %s blocks and %s chunks @ %s b/rec',
            'spilled' if disk else 'serialized', '.'.join(map(str, self.id)),
            bytes_spilled / 1024 / 1024, elements_spilled, blocks_spilled, chunks_spilled, self.element_size)

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
        if len(self) > PARTITIONED_SORT_SPLITS * 2 and self.key not in (None, identity):
            batches = list(batch_data(self, len(self) // PARTITIONED_SORT_SPLITS))
            for b in batches:
                b.sort(key=self.key)
            return merge(*batches, key=self.key)
        else:
            self.sort(key=self.key)
            return list.__iter__(self)



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

    # Indicate that this data set must be synchronized on. The tasks for the
    # ShuffleReadingDataset which read from this data set need to wait until
    # each and every shuffle write task has completed, and the dependencies
    # are all to all.
    sync_required = True


    def __init__(self, src, pcount, partitioner=None, bucket=None, key=None, comb=None, *,
            block_size_mb=None, serialization='pickle', compression='lz4'):
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
        :param block_size_mb: float or None
            The size of the blocks to produce in serializing the shuffle data. Defaults to
            bndl.compute.shuffle.block_size_mb.
        :param serialization: str or None
            A string compatible to the serialization parameter of ContainerFactory. E.g.
            'pickle', 'marshal', 'text', 'binary', 'json', etc. Defaults to 'pickle'.
        :param compression: str or None
            A string compatible to the compression parameter of ContainerFactory. E.g. 'gzip'.
        '''
        super().__init__(src.ctx, src)
        self.pcount = pcount or len(src.parts())
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = bucket or SortedBucket
        self.key = key

        self.block_size_mb = block_size_mb or src.ctx.conf['bndl.compute.shuffle.block_size_mb_net']
        self.serialization = serialization
        self.compression = compression


    @property
    def cleanup(self):
        return self._cleanup


    def _cleanup(self, job):
        for executor in job.ctx.executors:
            executor.service('shuffle').clear_bucket(self.id)
        for worker in job.ctx.workers:
            worker.service('shuffle').clear_bucket(self.id)
        job.ctx.node.service('shuffle').clear_bucket(job.ctx.node, self.id)


    def parts(self):
        return [
            ShuffleWritingPartition(self, i, p)
            for i, p in enumerate(self.src.parts())
        ]


    @property
    @lru_cache()
    def disk_container(self):
        return ContainerFactory('disk', self.serialization, self.compression)


    @property
    @lru_cache()
    def memory_container(self):
        return ContainerFactory('memory', self.serialization, self.compression)



class ShuffleWritingPartition(Partition):
    def partitioner(self):
        # get / create the partitioner function possibly using a key function
        key = self.dset.key
        if key:
            partitioner = self.dset.partitioner
            def part_(element):
                return partitioner(key(element))
            return part_
        else:
            return self.dset.partitioner


    def _compute(self):
        # ensure that partitioner is portable
        if self.dset.partitioner is portable_hash:
            assert (os.environ.get('PYTHONHASHSEED') or 'random') != 'random', \
                'PYTHONHASHSEED must be set, otherwise shuffling will have incorrect output'

        logger.info('starting shuffle write of partition %r', self.id)

        executor = self.dset.ctx.node

        workers = executor.peers.filter(machine=executor.machine, node_type=('worker', 'driver'))
        assert workers
        workers.sort(key=lambda w: w.name, reverse=True)
        worker = workers[0]

        # create a bucket for each output partition
        buckets = [self.dset.bucket((self.dset.id, self.idx, out_idx), self.dset.key,
                                    self.dset.comb, self.dset.block_size_mb, self.dset.block_size_mb,
                                    self.dset.memory_container, self.dset.disk_container)
                   for out_idx in range(self.dset.pcount)]

        bucket_add = [bucket.add for bucket in buckets]
        bucket_count = len(buckets)

        bytes_serialized = 0
        elements_partitioned = 0

        def spill(nbytes):
            if nbytes <= 0:
                return 0

            nonlocal bytes_serialized

            buckets_by_size = sorted((b for b in buckets if len(b) > 0), key=attrgetter('memory_size'))
            if len(buckets_by_size) == 0:
                return 0

            spilled = 0
            while buckets_by_size:
                bucket = buckets_by_size.pop()
                spilled += bucket.serialize(True)
                if spilled >= nbytes:
                    break

            gc.collect(1)

            bytes_serialized += spilled
            if spilled:
                logger.debug('spilled %.2f mb', spilled / 1024 / 1024)
            return spilled

        memory_manager = executor.memory_manager
        partitioner = self.partitioner()
        src = self.src.compute()

        check_interval = 1024

        with memory_manager.async_release_helper(self.id, spill, priority=2) as memcheck:
            # add each element to the bucket assigned to by the partitioner
            # (limited by the bucket count and wrapped around)
            for element in src:
                bucket_add[partitioner(element) % bucket_count](element)
                elements_partitioned += 1
                if elements_partitioned % check_interval == 0:
                    if memcheck():
                        check_interval = 1
                    elif check_interval < 16384:
                        check_interval *= 2

            # serialize the buckets for shuffle read
            for bucket in buckets:
                bytes_serialized += bucket.serialize(False)
                for block in flatten(bucket.batches):
                    if hasattr(block, 'to_disk'):
                        memory_manager.add_releasable(block.to_disk, block.id, 1, block.size)
                memcheck()

        batches = [bucket.batches for bucket in buckets]
        try:
            worker.service('shuffle').set_buckets(self.id, batches).result()
        except Exception:
            logger.exception('Unable to send buckets to worker/driver for part %s.%s',
                             self.dset.id, self.idx)
            raise

        logger.info('partitioned %s.%s of %s elem\'s, serialized %.1f mb',
                    self.dset.id, self.idx, elements_partitioned, bytes_serialized / 1024 / 1024)

        # set the result location for _compute_part
        task_context()['result_location'] = worker.name



class ShuffleReadingDataset(Dataset):
    '''
    The reading side of a shuffle.

    It is expected that the source of ShuffleReadingDataset is a ShuffleWritingDataset
    (or at least compatible to it). Many properties of it are read in the shuffle read, i.e.:
    block_size_mb, key, bucket.sorted, memory_container, disk_container
    '''
    def __init__(self, src, sorted=None):
        super().__init__(src.ctx, src)
        self.sorted = sorted


    @lru_cache()
    def parts(self):
        sources = self.src.parts()
        return [
            ShuffleReadingPartition(self, i, sources)
            for i in range(self.src.pcount)
        ]



class ShuffleReadingPartition(Partition):
    def __init__(self, dset, idx, src):
        super().__init__(dset, idx)
        self.src_count = len(src)


    def get_sources(self):
        executor = self.dset.ctx.node
        dependency_locations = task_context()['dependency_locations']

        # if a dependency wasn't executed yet (e.g. cache after shuffle)
        # raise dependencies failed for restart
        if None in dependency_locations:
            unknown = chain.from_iterable(dependency_locations[None].values())
            unknown = sortgroupby(unknown, getter(0))
            unknown = ['%s:[%s]' % (dset_id, ','.join(map(str, sorted(pluck(1, deps)))))
                       for dset_id, deps in unknown]
            logger.warning('Unable to compute %s because locations are unknown for %s',
                           '.'.join(map(str, self.id)), ', '.join(unknown))
            raise DependenciesFailed({None: dependency_locations[None]})

        source_names = set(dependency_locations.keys())
        assert executor.name not in source_names, "Executors don't host shuffle results, and" \
                                                  " should thus not appear as source: %r" \
                                                  % source_names

        # sort the source executor names relative to the name of the local executor
        # if every node does this, load will be spread more evenly in reading blocks
        source_names = sorted(source_names)
        split = bisect_left(source_names, executor.name)
        source_names = source_names[split:] + source_names[:split]

        sources = []
        dependencies_missing = {}
        # map the source names to actual peer objects
        # mark dependencies as failed for unknown or unconnected peers
        for source_name in source_names:
            source = executor.peers.get(source_name)
            if not source or not source.is_connected:
                dependencies_missing[source_name] = dependency_locations[source_name]
            else:
                sources.append(source)

        # abort and request re-computation of missing dependencies if any
        if dependencies_missing:
            raise DependenciesFailed(dependencies_missing)

        return sources


    def get_sizes(self):
        dependency_locations = task_context()['dependency_locations']
        dependencies_missing = {}

        sources = self.get_sources()
        # issue requests for the bucket bucket prep and get back sizes
        size_requests = [source.service('shuffle').get_bucket_sizes(self.dset.src.id, self.idx)
                         for source in sources]

        sizes = []
        # wait for responses and zip with a function to get a block
        # if a node is not connected, add it to the missing dependencies
        for source, future in zip(sources, size_requests):
            try:
                size = future.result()
            except NotConnected:
                # mark all dependencies of node as missing
                dependencies_missing[source.name] = dependency_locations[source.name]
            except InvocationException:
                logger.exception('Unable to compute bucket size %s.%s on %s' %
                                 (self.dset.src.id, self.idx, source.name))
                raise
            except Exception:
                logger.exception('Unable to compute bucket size %s.%s on %s' %
                                 (self.dset.src.id, self.idx, source.name))
                raise
            else:
                sizes.append((source, source.service('shuffle').get_bucket_blocks, size))

        # if size info is missing for any source partitions, fail computing this partition
        # and indicate which tasks/parts aren't available. This assumes that the task ids
        # for the missing source partitions equals the ids of these partitions.
        size_info_missing = set(range(self.src_count))
        # keep track of where a source partition is available
        source_locations = {}

        for source_idx, (source, get_blocks, size) in enumerate(sizes):
            selected = []
            for src_part_idx, block_sizes in size:
                if src_part_idx in size_info_missing:
                    size_info_missing.remove(src_part_idx)
                    source_locations[src_part_idx] = (source, block_sizes)
                    selected.append((src_part_idx, block_sizes))
                else:
                    other_executor, other_sizes = source_locations[src_part_idx]
                    logger.warning('Source partition %r.%r available more than once, '
                                   'at least at %s with block sizes %r and %s with block_sizes %r',
                                   self.dset.src.id, src_part_idx, source, block_sizes, other_executor, other_sizes)
            sizes[source_idx] = source, get_blocks, selected

        # translate size info missing into missing dependencies
        if size_info_missing:
            for src_idx in list(size_info_missing):
                for source, dependencies in dependency_locations.items():
                    for executor, dependencies in dependencies.items():
                        for dep_dset_id, dep_part_idx in dependencies:
                            assert dep_dset_id == self.dset.src.id
                            if dep_part_idx == src_idx:
                                from pprint import pprint
                                pprint(dependencies_missing)
                                dependencies_missing \
                                    .setdefault(source, {}) \
                                    .setdefault(executor, set()) \
                                    .add((dep_dset_id, src_idx))
                                size_info_missing.remove(src_idx)
                                break
        if size_info_missing:
            raise Exception('Bucket size information from %r could not be retrieved, '
                            'but can\'t raise DependenciesFailed as one or more source '
                            'partition ids are not found in dependency_locations %r' %
                            (size_info_missing, dependency_locations))

        # raise DependenciesFailed to trigger re-execution of the missing dependencies and
        # subsequently the computation of _this_ partition
        if dependencies_missing:
            raise DependenciesFailed(dependencies_missing)

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
            logger.info('shuffling %.1f mb (%s batches, %s blocks) from %s executors',
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

        for executor, get_blocks, parts in self.get_sizes():
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
            for request in star_prefetch(get_blocks, requests):
                try:
                    blocks = request.result()
                except TaskCancelled:
                    raise
                except NotConnected:
                    # consider all data from the executor lost
                    failed = task_context()['dependency_locations'][executor.name]
                    raise DependenciesFailed({executor.name: failed})
                except Exception:
                    logger.exception('unable to retrieve blocks from %s', executor.name)
                    raise
                else:
                    yield from blocks


    def merge_sorted(self, blocks):
        bucket = self.dset.src.bucket(
            (self.dset.id, self.idx),
            self.dset.src.key, None,
            self.dset.ctx.conf['bndl.compute.shuffle.block_size_mb_merge'],
            self.dset.ctx.conf['bndl.compute.shuffle.chunk_size_mb_merge'],
            self.dset.src.memory_container,
            self.dset.src.disk_container
        )

        bytes_received = 0

        def spill(nbytes):
            spilled = bucket.serialize(True)
            logger.debug('spilled %.2f mb', spilled / 1024 / 1024)
            return spilled

        with self.dset.ctx.node.memory_manager.async_release_helper(self.id, spill, priority=3) as memcheck:
            for block in blocks:
                bytes_received += block.size
                bucket.extend(block.read())
                memcheck()

        if not bucket.batches:
            logger.info('sorted %.1f mb', bytes_received / 1024 / 1024)
            return iter(bucket)
        else:
            spill(0)

            logger.info('merge sorting %.1f mb from %s blocks in %s batches', bytes_received / 1024 / 1024,
                        sum(1 for batch in bucket.batches for block in batch), len(bucket.batches))

            def _block_stream(batch):
                for block in reversed(batch):
                    for chunk in block.read_all():
                        yield from chunk
                    block.clear()

            streams = list(map(_block_stream, bucket.batches))
            s = merge(*streams, key=self.dset.src.key)

            return s


    def _compute(self):
        sort = self.dset.sorted or self.dset.src.bucket.sorted

        logger.info('starting %s shuffle read of partition %r',
                    'sorted' if sort else 'unsorted', self.id)

        # create a stream of blocks
        blocks = self.blocks()

        if sort:
            return self.merge_sorted(blocks)
        else:
            return chain.from_iterable(block.read() for block in blocks)


    def _locality(self, executors):
        return ()



class ShuffleManager(object):
    def __init__(self, node):
        self.node = node

        # output buckets structured as:
        # - dict keyed by: shuffle write data set id
        # - dict keyed by: source partition index
        # - list of batches per destination partition (1)
        # - list of batches
        # - list of blocks
        #
        # (1) indexed by destination partition in shuffle read data set
        self.buckets = {}


    @rmi.direct
    def set_buckets(self, src, part_id, buckets):
        '''
        Set the buckets for the data set of the given partition (after shuffle write).
        :param part: The partition of the source (shuffle writing) data set.
        :param buckets: The buckets computed for the partition.
        '''
        # expose the blocks (in the batches in the buckets) to memory management if they're
        # in-memory blocks, (e.g. SerializedInMemory or SharedMemoryContainer)
        memory_manager = self.node.memory_manager
        for block in flatten(buckets):
            if hasattr(block, 'to_disk'):
                memory_manager.add_releasable(block.to_disk, block.id, 1, block.size)

        self.buckets.setdefault(part_id[0], {})[part_id[1]] = buckets


    def _buckets_for_dset(self, src_dset_id):
        buckets = self.buckets.get(src_dset_id)
        if buckets is None:
            raise KeyError('No buckets for dataset %r' % src_dset_id)
        return buckets


    @rmi.direct
    def get_bucket_sizes(self, src, src_dset_id, dest_part_idx):
        '''
        Return the sizes and coordinates of the buckets for the destination partition.

        :param src: The (rmi) peer node requesting the finalization.
        :param dset_id: The id of the source data set.
        :param dest_part_idx: The index of the destination partition.
        '''
        try:
            dset_buckets = self._buckets_for_dset(src_dset_id)
        except KeyError:
            return []
        else:
            sizes = []
            for src_part_idx, buckets in dset_buckets.items():
                bucket = buckets[dest_part_idx]
                bucket_sizes = [[block.size for block in batch]
                                for batch in bucket]
                sizes.append((src_part_idx, bucket_sizes))
            return sizes


    @rmi.direct
    def get_bucket_block(self, src, src_dset_id, src_part_idx, dest_part_idx, batch_idx, block_idx):
        '''
        Retrieve a block from a batch in a bucket.
        '''
        try:
            buckets = self._buckets_for_dset(src_dset_id)
        except KeyError:
            logger.error('Unable to return block, buckets for dset %s missing', src_dset_id)
            raise

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
        try:
            batch = bucket[batch_idx]
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


    @rmi.direct
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
        if self.buckets.pop(dset_id, None):
            gc.collect()
