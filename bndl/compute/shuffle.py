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
from collections import defaultdict
from concurrent.futures import Future
from functools import lru_cache
from itertools import chain
from math import ceil
from operator import attrgetter
from statistics import mean
import gc
import logging
import os

from cytoolz.itertoolz import merge_sorted, pluck

from bndl import rmi
from bndl.compute.dataset import Dataset, Partition
from bndl.compute.storage import StorageContainerFactory, InMemory
from bndl.execute import DependenciesFailed, TaskCancelled
from bndl.execute.worker import task_context
from bndl.net.connection import NotConnected
from bndl.rmi import InvocationException
from bndl.util.collection import batch as batch_data, ensure_collection
from bndl.util.conf import Float
from bndl.util.funcs import star_prefetch
from bndl.util.hash import portable_hash


logger = logging.getLogger(__name__)


block_size_mb = Float(4, desc='Target (maximum) size (in megabytes) of blocks created by spilling'
                              '/ serializing elements to disk')


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
        try:
            test_set = data[:max(10, len(data) // 1000)]
            c = self.memory_container(('tmp',))
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

        # split into several blocks if necessary
        block_size_recs = ceil(self.block_size_mb * 1024 * 1024 / self.element_size)
        if block_size_recs > len(data):
            return [data]
        else:
            return list(batch_data(data, block_size_recs))


    def serialize(self, disk):
        '''
        Serialize the elements in this bucket (not in the memory/disk blocks) to disk or in blocks
        in memory. The elements are serialized into a new batch of one ore more blocks
        (approximately) at most self.block_size_mb large.

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
        batch_no = len(self.batches)

        Container = self.disk_container if disk else self.memory_container

        blocks = self._to_blocks()
        while blocks:
            block = blocks.pop()
            container = Container(self.id + ('%r.%r' % (batch_no, len(batch)),))

            container.write(block)
            batch.append(container)

            blocks_spilled += 1
            elements_spilled += len(block)
            bytes_spilled += container.size

        # recalculate the size of the elements given the bytes and elements just spilled
        # the re-estimation is dampened in a decay like fashion
        self.element_size = (self.element_size + bytes_spilled // elements_spilled) // 2

        logger.debug('%s bucket %s of %.2f mb and %s recs to %s blocks @ %s b/rec',
            'spilled' if disk else 'serialized', '.'.join(map(str, self.id)),
            bytes_spilled / 1024 / 1024, elements_spilled, blocks_spilled, self.element_size)

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

        self.block_size_mb = block_size_mb or src.ctx.conf['bndl.compute.shuffle.block_size_mb']
        self.serialization = serialization
        self.compression = compression


    @property
    def cleanup(self):
        return self._cleanup


    def _cleanup(self, job):
        requests = [worker.service('shuffle').clear_bucket(self.id)
                    for worker in job.ctx.workers]
#         for request in requests:
#             request.result()


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

        worker = self.dset.ctx.node
        memory = worker.memory

        # create a bucket for each output partition
        buckets = [self.dset.bucket((self.dset.id, self.idx, out_idx), self.dset.key,
                                    self.dset.comb, self.dset.block_size_mb,
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

            gc.collect()

            bytes_serialized += spilled
            if spilled:
                logger.debug('spilled %.2f mb', spilled / 1024 / 1024)
            return spilled

        partitioner = self.partitioner()

        with memory.async_release_helper(self.id, spill, priority=2) as memcheck:
            # add each element to the bucket assigned to by the partitioner
            # (limited by the bucket count and wrapped around)
            for element in self.src.compute():
                bucket_add[partitioner(element) % bucket_count](element)
                elements_partitioned += 1
                memcheck()

            # serialize the buckets for shuffle read
            for bucket in buckets:
                bytes_serialized += bucket.serialize(False)
                for batch in bucket.batches:
                    for block in batch:
                        if isinstance(block, InMemory):
                            memory.add_releasable(block.to_disk, block.id, 0, block.size)
                memcheck()

        worker.service('shuffle').set_buckets(self, buckets)

        logger.info('partitioned %s.%s of %s elem\'s, serialized %.1f mb',
                    self.dset.id, self.idx, elements_partitioned, bytes_serialized / 1024 / 1024)



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
        worker = self.dset.ctx.node

        dependency_locations = task_context()['dependency_locations']

        # if a dependency wasn't executed yet (e.g. cache after shuffle)
        # raise dependencies failed for restart
        if None in dependency_locations:
            logger.warning('Unable to compute %r because dependency locations are unknown for %r',
                           self, dependency_locations[None])
            raise DependenciesFailed({None: dependency_locations[None]})

        source_names = set(dependency_locations.keys())

        local_source = worker.name in source_names
        if local_source:
            source_names.remove(worker.name)

        # sort the source worker names relative to the name of the local worker
        # if every node does this, load will be spread more evenly in reading blocks
        source_names = sorted(source_names)
        split = bisect_left(source_names, worker.name)
        source_names = source_names[split:] + source_names[:split]

        peers = worker.peers
        sources = []
        dependencies_missing = {}

        # map the source names to actual peer objects
        # mark dependencies as failed for unknown or unconnected peers
        for peer_name in source_names:
            peer = peers.get(peer_name)
            if not peer or not peer.is_connected:
                dependencies_missing[peer_name] = dependency_locations[peer_name]
            else:
                sources.append(peer)

        # abort and request re-computation of missing dependencies if any
        if dependencies_missing:
            raise DependenciesFailed(dependencies_missing)

        return local_source, sources


    def get_local_sizes(self):
        node = self.dset.ctx.node
        shuffle_svc = node.service('shuffle')

        # make it seem like fetching locally is remote
        # so it fits in the stream_batch loop
        def get_local_block(*args):
            fut = Future()
            try:
                fut.set_result(shuffle_svc.get_bucket_blocks(node, *args))
            except Exception as e:
                fut.set_exception(e)
            return fut

        local_sizes = shuffle_svc.get_bucket_sizes(node, self.dset.src.id, self.idx)
        return (node, get_local_block, local_sizes)


    def get_sizes(self):
        dependency_locations = task_context()['dependency_locations']
        dependencies_missing = defaultdict(set)

        local_source, sources = self.get_sources()
        sizes = []

        # add the local fetch operations if the local node is a source
        if local_source:
            sizes.append(self.get_local_sizes())

        # issue requests for the bucket bucket prep and get back sizes
        size_requests = [worker.service('shuffle').get_bucket_sizes(self.dset.src.id, self.idx)
                         for worker in sources]

        # wait for responses and zip with a function to get a block
        # if a worker is not connected, add it to the missing dependencies
        for worker, future in zip(sources, size_requests):
            try:
                size = future.result()
            except NotConnected:
                # mark all dependencies of worker as missing
                dependencies_missing[worker.name] = set(dependency_locations[worker.name])
            except InvocationException:
                logger.exception('Unable to compute bucket size %s.%s on %s' %
                                 (self.dset.src.id, self.idx, worker.name))
                raise
            except Exception:
                logger.exception('Unable to compute bucket size %s.%s on %s' %
                                 (self.dset.src.id, self.idx, worker.name))
                raise
            else:
                sizes.append((worker, worker.service('shuffle').get_bucket_blocks, size))

        # if size info is missing for any source partitions, fail computing this partition
        # and indicate which tasks/parts aren't available. This assumes that the task ids
        # for the missing source partitions equals the ids of these partitions.
        size_info_missing = set(range(self.src_count))
        # keep track of where a source partition is available
        source_locations = {}

        for worker_idx, (worker, get_blocks, size) in enumerate(sizes):
            selected = []
            for src_part_idx, block_sizes in size:
                if src_part_idx in size_info_missing:
                    size_info_missing.remove(src_part_idx)
                    source_locations[src_part_idx] = (worker, block_sizes)
                    selected.append((src_part_idx, block_sizes))
                else:
                    other_worker, other_sizes = source_locations[src_part_idx]
                    logger.warning('Source partition %r.%r available more than once, '
                                   'at least at %s with block sizes %r and %s with block_sizes %r',
                                   self.dset.src.id, src_part_idx, worker, block_sizes, other_worker, other_sizes)
            sizes[worker_idx] = worker, get_blocks, selected

        # translate size info missing into missing dependencies
        if size_info_missing:
            for src_idx in list(size_info_missing):
                for worker, dependencies in dependency_locations.items():
                    for dep_dset_id, dep_part_idx in dependencies:
                        assert dep_dset_id == self.dset.src.id
                        if dep_part_idx == src_idx:
                            dependencies_missing[worker].add((dep_dset_id, src_idx))
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

        # # print some workload info
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

        for worker, get_blocks, parts in self.get_sizes():
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
                    # consider all data from the worker lost
                    failed = task_context()['dependency_locations'][worker.name]
                    raise DependenciesFailed({worker.name: failed})
                except Exception:
                    logger.exception('unable to retrieve blocks from %s', worker.name)
                    raise
                else:
                    yield from blocks



    def merge_sorted(self, blocks):
        bucket = self.dset.src.bucket(
            (self.dset.id, self.idx),
            self.dset.src.key, None,
            self.dset.src.block_size_mb,
            self.dset.src.memory_container,
            self.dset.src.disk_container
        )

        def spill(nbytes):
            spilled = bucket.serialize(True)
            logger.debug('spilled %.2f mb', spilled / 1024 / 1024)
            return spilled

        bytes_received = 0
        with self.dset.ctx.node.memory.async_release_helper(self.id, spill, priority=1) as memcheck:
            for block in blocks:
                bytes_received += block.size
                data = block.read()
                bucket.extend(data)
                memcheck()

                logger.debug('received block of %.2f mb, %r items',
                             block.size / 1024 / 1024, len(data))

            logger.info('sorting %.1f mb', bytes_received / 1024 / 1024)

        if not bucket.batches:
            return iter(bucket)
        else:
            streams = [bucket]
            for batch in bucket.batches:
                streams.append(chain.from_iterable(block.read() for block in reversed(batch)))
            return merge_sorted(*streams, key=self.dset.src.key)


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


    def _locality(self, workers):
        return ()



class ShuffleManager(object):
    def __init__(self, worker):
        self.worker = worker

        # output buckets structured as:
        # - dict keyed by: shuffle write data set id
        # - dict keyed by: source partition index
        # - dict keyed by: destination partition index (in shuffle read data set)
        # - list of batches
        # - list of blocks
        self.buckets = {}


    def set_buckets(self, part, buckets):
        '''
        Set the buckets for the data set of the given partition (after shuffle write).
        :param part: The partition of the source (shuffle writing) data set.
        :param buckets: The buckets computed for the partition.
        '''
        self.buckets.setdefault(part.dset.id, {})[part.idx] = [bucket.batches for bucket in buckets]


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
        self.buckets.pop(dset_id, None)
        gc.collect()
