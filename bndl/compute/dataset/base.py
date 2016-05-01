from bndl.util.cython import try_pyximport_install ; try_pyximport_install()

import abc
import bisect
from functools import partial, total_ordering
from itertools import islice, product, chain
import logging
import random

from bndl.compute.schedule import schedule_job
from bndl.util import serialize
from bndl.util.collection import non_empty, getter
from bndl.util.funcs import identity
import collections.abc as collections_abc
from cytoolz import pluck  # @UnresolvedImport
import sortedcontainers.sortedlist


try:
    from bndl.compute.dataset.stats import partition_size, local_mean, reduce_mean
    from bndl.util.hash import portable_hash
except ImportError as e:
    raise ImportError('Unable to load Cython extensions, install Cython or use a binary distribution') from e


logger = logging.getLogger(__name__)


class Dataset(metaclass=abc.ABCMeta):
    def __init__(self, ctx, src=None, dset_id=None):
        self.ctx = ctx
        self.src = src
        self.id = dset_id or next(ctx._dataset_ids)
        self._cache = False
        self._worker_preference = None
        self._worker_filter = None


    @abc.abstractmethod
    def parts(self):
        pass


    @property
    def sync_required(self):
        return False


    @property
    def cleanup(self):
        return None


    def map(self, op):
        return ElementTransformingDataset(self.ctx, self, op)


    def pluck(self, ind, default=None):
        return self.map_partitions(lambda p:
                                   pluck(ind, p, default=default)
                                   if default else pluck(ind, p))


    def flatmap(self, op):
        return self.map(op).map_partitions(lambda i: chain.from_iterable(i))


    def map_partitions(self, op):
        return self.map_partitions_with_part(lambda p, iterator: op(iterator))


    def map_partitions_with_index(self, op):
        return self.map_partitions_with_part(lambda p, iterator: op(p.idx, iterator))


    def map_partitions_with_part(self, op):
        return PartitionTransformingDataset(self.ctx, self, op)


    def filter(self, op=None):
        return FilteringDataset(self.ctx, self, op)


    def mask_partitions(self, mask):
        return MaskedDataset(self, mask)


    def key_by(self, key):
        return self.map(lambda e: (key(e), e))


    def values_as(self, val):
        return self.map(lambda e: (e, val(e)))


    def keys(self):
        return self.pluck(0)


    def values(self):
        return self.pluck(1)


    def map_keys(self, op):
        return self.map(lambda kv: (op(kv[0]), kv[1]))


    def flatmap_values(self, op):
        return self.values().flatmap(op)


    def map_values(self, op):
        return self.map(lambda kv: (kv[0], op(kv[1])))



    def first(self):
        # TODO is this leaving _request tasks hanging on workers?
        return next(self.itake(1))


    def take(self, num):
        # TODO don't use itake if first partition doesn't yield > 50% of num
        return list(self.itake(num))


    def itake(self, num):
        sliced_parts = self.map_partitions(lambda p: islice(p, num))
        yield from islice(sliced_parts.icollect(eager=False), num)


    def stat(self, f):
        try:
            return next(self.reduce(lambda seq: (f(seq),), pcount=1).icollect())
        except StopIteration:
            raise ValueError('dataset is empty')


    def count(self):
        return sum(self.map_partitions(partition_size).icollect())


    def sum(self):
        return self.stat(sum)


    def min(self):
        return self.stat(min)


    def max(self):
        return self.stat(max)


    def mean(self):
        means = self.map_partitions(local_mean).icollect()
        total, count = reduce_mean(means)
        return total / count



    def union(self, other):
        return UnionDataset(self, other)


    def group_by(self, key, partitioner=None):
        return (self
            .key_by(key)
            .group_by_key(partitioner=partitioner)
            .map(lambda group: (
                group[0],
                list(pluck(1, group[1]))
            ))
        )


    def group_by_key(self, partitioner=None):
        def sort_and_group(partition):
            partition = sorted(partition, key=getter(0))
            if not partition:
                return ()
            key = partition[0][0]
            group = []
            for e in partition:
                if key == e[0]:
                    group.append(e)
                else:
                    yield key, group
                    group = [e]
                    key = e[0]
            yield key, group

        return (self
            .shuffle_by(key=getter(0), partitioner=partitioner)
            .map_partitions(sort_and_group)
        )



    def join_on(self, other, key, partitioner=None):
        a = self.key_by(key)
        b = other.key_by(key)
        return a.join(b, partitioner=partitioner)


    def join(self, other, partitioner=None):
        a = self.map_values(lambda v: (1, v))
        b = other.map_values(lambda v: (2, v))
        def local_join(group):
            key, group = group
            left, right = [], []
            for (i, v) in pluck(1, group):
                if i == 1:
                    left.append(v)
                elif i == 2:
                    right.append(v)
            if left and right:
                return key, list(product(left, right))
        return a.union(b).group_by_key(partitioner=partitioner).map(local_join).filter()



    def distinct(self, pcount=None, partitioner=None):
        return self.reduce(set, pcount, partitioner=partitioner, bucket=SetBucket)



    def sort(self, pcount=None, key=identity, reverse=False):
        pcount = pcount or self.ctx.default_pcount
        # TODO if sort into 1 partition

        dset_size = self.count()
        if dset_size == 0:
            return self

        # sample to find a good distribution over buckets
        fraction = min(pcount * 20. / dset_size, 1.)
        samples = self.sample(fraction).collect()
        # apply the key function if any
        if key: samples = map(key, samples)
        # sort the samples to function as boundaries
        samples = sorted(set(samples), reverse=reverse)
        # take pcount - 1 points evenly spaced from the samples as boundaries
        boundaries = [samples[len(samples) * (i + 1) // pcount] for i in range(pcount - 1)]
        # and use that in the range partitioner to shuffle
        partitioner = RangePartitioner(boundaries, reverse)
        shuffled = self.shuffle_by(pcount, partitioner=partitioner, key=key)
        # finally sort within the partition
        return shuffled.map_partitions(partial(sorted, key=key, reverse=reverse))



    def reduce(self, op, pcount=None, partitioner=None, bucket=None):
        return self.aggregate(op, op, pcount, partitioner, bucket)


    def aggregate(self, comb, agg, pcount=None, partitioner=None, bucket=None):
        shuffle = ShuffleWritingDataset(self.ctx, self, pcount, partitioner, comb=comb)
        return AggregatingDataset(self.ctx, shuffle, agg)


    def shuffle(self, pcount=None, partitioner=None, bucket=None, comb=None):
        return self.shuffle_by(pcount, partitioner, bucket, identity)


    def shuffle_by(self, pcount=None, partitioner=None, bucket=None, key=None, comb=None):
        shuffle = ShuffleWritingDataset(self.ctx, self, pcount, partitioner, bucket, key, comb)
        return ShuffleReadingDataset(self.ctx, shuffle)



    def sample(self, fraction, seed=None):
        # TODO implement sampling with_replacement
        # TODO implement stratified sampling
        rng = random.Random(seed)
        return self.filter(lambda e: rng.random() < fraction)



    def collect(self, parts=False):
        return list(self.icollect(parts=parts))


    def collect_as_map(self, parts=False):
        if parts:
            return list(map(dict, self.icollect(parts=True)))
        else:
            return dict(self.icollect())


    def collect_as_set(self):
        return set(self.icollect())


    def icollect(self, eager=True, parts=False):
        result = self._execute(eager)
        result = filter(lambda part: part is not None, result)
        if not parts:
            result = chain.from_iterable(result)
        yield from result


    def foreach(self, f):
        for e in self.icollect():
            f(e)


    def execute(self):
        for _ in self._execute():
            pass

    def _execute(self, eager=True):
        yield from self.ctx.execute(schedule_job(self), eager=eager)


    def prefer_workers(self, fltr):
        return self._with('_worker_preference', fltr)

    def allow_workers(self, fltr):
        return self._with('_worker_filter', fltr)

    def require_local_workers(self):
        return self.allow_workers(lambda workers: [w for w in workers if w.islocal])

    def allow_all_workers(self):
        return self.allow_workers(None)


    def cache(self, cached=True):
        assert self.ctx.node.node_type == 'driver'
        self._cache = cached
        if not cached:
            tasks = [w.uncache_dset(self.id) for w in self.ctx.workers]
            [task.result() for task in tasks]

        return self

    @property
    def cached(self):
        return self._cache


    def __del__(self):
        if hasattr(self, '_cache') and self._cache:
            if self.ctx.node.node_type == 'driver':
                self.cache(False)


    def __hash__(self):
        return self.id


    def __eq__(self, other):
        return self.id == other.id


    def _with(self, attribute, value):
        clone = type(self).__new__(type(self))
        clone.__dict__ = dict(self.__dict__)
        setattr(clone, attribute, value)
        return clone


    def __str__(self):
        return 'dataset %s' % self.id



@total_ordering
class Partition(metaclass=abc.ABCMeta):
    def __init__(self, dset, idx, src=None):
        self.dset = dset
        self.idx = idx
        self.src = src

    def materialize(self, ctx):
        worker = ctx.node

        # check cache
        if self.dset.cached and self.dset.id in worker.dset_cache:
            dset_cache = worker.dset_cache[self.dset.id]
            if self.idx in dset_cache:
                return dset_cache[self.idx]

        data = self._materialize(ctx)

        # cache if requested
        if self.dset.cached:
            if not isinstance(data, collections_abc.Sequence):
                data = list(data)
            dset_cache = worker.dset_cache.setdefault(self.dset.id, {})
            dset_cache[self.idx] = data

        # return data
        return data

    @abc.abstractmethod
    def _materialize(self, ctx):
        pass


    def preferred_workers(self, workers):
        if self.dset._worker_preference:
            return self.dset._worker_preference(workers)
        elif self.src:
            return self.src.preferred_workers(workers)

    def allowed_workers(self, workers):
        if self.dset._worker_filter:
            return self.dset._worker_filter(workers)
        elif self.src:
            return self.src.allowed_workers(workers)


    def __lt__(self, other):
        return other.dset.id < self.dset.id or other.idx > self.idx

    def __eq__(self, other):
        return other.dset.id == self.dset.id and other.idx == self.idx

    def __hash__(self):
        return hash((self.dset.id, self.idx))

    def __str__(self):
        return '%s(%s.%s)' % (self.__class__.__name__, self.dset.id, self.idx)



class IterablePartition(Partition):
    def __init__(self, dset, idx, iterable):
        super().__init__(dset, idx)
        self.iterable = iterable

    # TODO look into e.g. https://docs.python.org/3.4/library/pickle.html#persistence-of-external-objects
    # for attachments? Or perhaps separate the control and the data paths?
    def __getstate__(self):
        state = dict(self.__dict__)
        iterable = state.pop('iterable')
        state['iterable'] = serialize.dumps(iterable)
        return state

    def __setstate__(self, state):
        iterable = state.pop('iterable')
        self.__dict__.update(state)
        self.iterable = serialize.loads(*iterable)

    def _materialize(self, ctx):
        return self.iterable


class MaskedDataset(Dataset):
    def __init__(self, src, mask):
        super().__init__(src.ctx, src)
        self.mask = mask

    def parts(self):
        return self.mask(self.src.parts())



class UnionDataset(Dataset):
    def __init__(self, *src):
        super().__init__(src[0].ctx, src)

    def union(self, other):
        extra = other.src if isinstance(other, UnionDataset) else(other,)
        return UnionDataset(*(self.src + extra))

    def parts(self):
        return list(chain.from_iterable(src.parts() for src in self.src))



class Partitioner(object, metaclass=abc.ABCMeta):
    def __init__(self, pcount, key=None):
        self.pcount = pcount
        self.key = key or identity

    def create_buckets(self, N):
        return [self.create_bucket() for _ in range(N)]

    @abc.abstractmethod
    def __call__(self, v):
        ...


class Buckets(object, metaclass=abc.ABCMeta):
    def __init__(self, pcount, key=None):
        self.pcount = pcount
        self.key = key or identity

    def __call__(self, N):
        return [self.create_bucket() for _ in range(N)]

    @abc.abstractmethod
    def create_bucket(self):
        ...


class ListBucket(list):
    add = list.append

class SetBucket(set):
    extend = set.update

class SortedListBucket(sortedcontainers.sortedlist.SortedList):
    def add_all(self, iterable):
        self.update(iterable)
        return self



class RangePartitioner():
    def __init__(self, boundaries, reverse=False):
        self.boundaries = boundaries
        self.reverse = reverse

    def __call__(self, v):
        b = bisect.bisect_left(self.boundaries, v)
        return len(self.boundaries) - b if self.reverse else b



class ShuffleWritingDataset(Dataset):
    def __init__(self, ctx, src, pcount, partitioner=None, bucket=None, key=None, comb=None):
        super().__init__(ctx, src)
        self.pcount = pcount or len(self.src.parts())
        self.comb = comb
        self.partitioner = partitioner or portable_hash
        self.bucket = ListBucket
        self.key = key or identity


    @property
    def sync_required(self):
        return True

    @property
    def cleanup(self):
        def _cleanup(job):
            futures = [worker.clear_bucket(self.id) for worker in job.ctx.workers]
            for f in futures:
                try:
                    f.result()
                except:
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


    def _ensure_buckets(self, worker):
        # TODO lock
        buckets = worker.buckets.get(self.dset.id)
        if not buckets:
            buckets = [self.dset.bucket() for _ in range(self.dset.pcount)]
            worker.buckets[self.dset.id] = buckets
        return buckets


    def _materialize(self, ctx):
        worker = self.dset.ctx.node
        buckets = self._ensure_buckets(worker)
        if len(buckets) > 1:
            key = self.dset.key
            partitioner = self.dset.partitioner

            if key:
                def select_bucket(e):
                    return partitioner(key(e))
            else:
                def select_bucket(e):
                    return partitioner(e)

            for e in self.src.materialize(ctx):
                buckets[select_bucket(e) % len(buckets)].add(e)

            if self.dset.comb:
                for k, b in enumerate(buckets):
                    if b:
                        buckets[k] = bucket = self.dset.bucket()
                        bucket.extend(self.dset.comb(b))
        else:
            data = self.src.materialize(ctx)
            if self.dset.comb:
                data = self.dset.comb(data)
            buckets[0].extend(data)



class ShuffleReadingDataset(Dataset):
    def __init__(self, ctx, src):
        super().__init__(ctx, src)
        assert isinstance(src, ShuffleWritingDataset)


    def bucket(self, worker, idx):
        yield from chain.from_iterable(self._bucket(worker, idx))


    def _bucket(self, worker, idx):
        b = worker.get_bucket(None, self.src.id, idx)
        if b:
            yield b

        futures = [w.get_bucket(self.src.id, idx) for w in worker.peers.filter(node_type='worker')]

        for f in futures:
            # TODO timeout and reschedule
            yield f.result()

    def parts(self):
        return [
            ShuffleReadingPartition(self, i)
            for i in range(self.src.pcount)
        ]


class ShuffleReadingPartition(Partition):
    def _materialize(self, ctx):
        return self.dset.bucket(self.dset.ctx.node, self.idx)


class AggregatingDataset(ShuffleReadingDataset):
    def __init__(self, ctx, src, agg):
        super().__init__(ctx, src)
        self.agg = agg

    def parts(self):
        return [
            AggregatingPartition(self, i, self.agg)
            for i in range(self.src.pcount)
        ]


class AggregatingPartition(ShuffleReadingPartition):
    def __init__(self, dset, idx, op):
        super().__init__(dset, idx)
        self.op = op

    def _materialize(self, ctx):
        b = super()._materialize(ctx)
        b = non_empty(b)
        return self.op(b) if b else ()



class TransformingDataset(Dataset):
    def __init__(self, ctx, src, *ops):
        super().__init__(ctx, src)
        self.ops = ops

    def parts(self):
        transformer = self.partition_transformer(self.ops)
        return [
            TransformingPartition(self, i, part, transformer)
            for i, part in enumerate(self.src.parts())
        ]

    @abc.abstractmethod
    def partition_transformer(self, ops):
        pass



class ElementTransformer(object):
    def __init__(self, ops):
        self.ops = ops

    def __call__(self, p, iterator):
        for o in self.ops:
            iterator = map(o, iterator)
        return iterator


class ElementTransformingDataset(TransformingDataset):
    partition_transformer = ElementTransformer



class PartitionTransformer(object):
    def __init__(self, ops):
        self.ops = ops

    def __call__(self, p, iterator):
        for op in self.ops:
            iterator = op(p, iterator)
        if iterator is None:
            return ()
        else:
            return iterator


class PartitionTransformingDataset(TransformingDataset):
    partition_transformer = PartitionTransformer



class PartitionFilter(object):
    def __init__(self, ops):
        self.ops = ops

    def __call__(self, p, iterator):
        if self.ops:
            for o in self.ops:
                iterator = filter(o, iterator)
            return iterator
        else:
            return filter(None, iterator)


class FilteringDataset(TransformingDataset):
    partition_transformer = PartitionFilter



class TransformingPartition(Partition):
    def __init__(self, dset, idx, src, transformation):
        super().__init__(dset, idx, src)
        self.transformation = transformation

    def _materialize(self, ctx):
        return self.transformation(self.src, self.src.materialize(ctx))
