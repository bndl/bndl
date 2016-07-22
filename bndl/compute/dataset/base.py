from bndl.util.cython import try_pyximport_install; try_pyximport_install()

from bisect import bisect_left
from collections import Counter
from copy import copy
from functools import partial, total_ordering, reduce
from itertools import islice, product, chain, starmap
from math import sqrt, log
from operator import add
import abc
import heapq
import logging
import os
import pickle
import struct

from bndl.compute.schedule import schedule_job
from bndl.util import serialize, cycloudpickle
from bndl.util.collection import is_stable_iterable
from bndl.util.exceptions import catch
from bndl.util.funcs import identity, getter, key_or_getter
from cytoolz.itertoolz import pluck, take  # @UnresolvedImport
import numpy as np
import sortedcontainers.sortedlist


try:
    from bndl.compute.dataset.stats import iterable_size, Stats, \
        sample_with_replacement, sample_without_replacement
    from bndl.util.hash import portable_hash
    from bndl.util.hyperloglog import HyperLogLog
except ImportError as exc:
    raise ImportError('Unable to load Cython extensions, '
                      'install Cython or use a binary distribution') from exc


logger = logging.getLogger(__name__)



def _filter_local_workers(workers):
    return [w for w in workers if w.islocal]


def _as_bytes(obj):
    t = type(obj)
    if t == str:
        return obj.encode()
    elif t == tuple:
        return b''.join(_as_bytes(e) for e in obj)
    elif t == int:
        return obj.to_bytes(obj.bit_length(), 'little')
    elif t == float:
        obj = struct.pack('>f', obj)
        obj = struct.unpack('>l', obj)[0]
        return obj.to_bytes(obj.bit_length(), 'little')
    else:
        return bytes(obj)


class Dataset(metaclass=abc.ABCMeta):
    cleanup = None
    sync_required = False

    def __init__(self, ctx, src=None, dset_id=None):
        self.ctx = ctx
        self.src = src
        self.id = dset_id or next(ctx._dataset_ids)
        self._cache = False
        self._cache_locs = {}
        self._worker_preference = None
        self._worker_filter = None


    @abc.abstractmethod
    def parts(self):
        pass


    def map(self, func):
        '''
        Transform elements in this dataset one by one.

        :param func: callable(element)
            applied to each element of the dataset
        '''
        return self.map_partitions(partial(map, func))

    def starmap(self, func):
        '''
        Variadic form of map.

        :param func: callable(element)
            applied to each element of the dataset
        '''
        return self.map_partitions(partial(starmap, func))


    def pluck(self, ind, default=None):
        '''
        Pluck indices from each of the elements in this dataset.

        :param ind: obj or list
            The indices to pluck with.
        :param default: obj
            A default value.

        For example::

            >>> ctx.collection(['abc']*10).pluck(1).collect()
            ['b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b']
            >>> ctx.collection(['abc']*10).pluck([1,2]).collect()
            [('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c'), ('b', 'c')]

        '''
        kwargs = {'default': default} if default is not None else {}
        return self.map_partitions(lambda p: pluck(ind, p, **kwargs))


    def flatmap(self, func=None):
        '''
        Transform the elements in this dataset into iterables and chain them
        within each of the partitions.

        :param func:
            The transformation to apply. Defaults to none; i.e. consider the
            elements in this the iterables to chain.

        For example::

            >>> ''.join(ctx.collection(['abc']*10).flatmap().collect())
            'abcabcabcabcabcabcabcabcabcabc'

        or::

            >>> import string
            >>> ''.join(ctx.range(5).flatmap(lambda i: string.ascii_lowercase[i-1]*i).collect())
            'abbcccdddd'

        '''
        iterables = self.map(func) if func else self
        return iterables.map_partitions(lambda iterable: chain.from_iterable(iterable))


    def map_partitions(self, func):
        '''
        Transform the partitions of this dataset.

        :param func: callable(iterator)
            The transformation to apply.
        '''
        return self.map_partitions_with_part(lambda p, iterator: func(iterator))


    def map_partitions_with_index(self, func):
        '''
        Transform the partitions - with their index - of this dataset.

        :param func: callable(index, iterator)
            The transformation to apply on the partition index and the iterator
            over the partition's elements.
        '''
        return self.map_partitions_with_part(lambda p, iterator: func(p.idx, iterator))


    def map_partitions_with_part(self, func):
        '''
        Transform the partitions - with the partition object as argument - of
        this dataset.

        :param func: callable(partition, iterator)
            The transformation to apply on the partition object and the iterator
            over the partition's elements.
        '''
        return TransformingDataset(self.ctx, self, func)


    def filter(self, func=None):
        '''
        Filter out elements from this dataset

        :param func: callable(element
            The test function to filter this dataset with. An element is
            retained in the dataset if the test is positive.
        '''
        return self.map_partitions(partial(filter, func))


    def mask_partitions(self, mask):
        '''
        :warning: experimental, don't use
        '''
        return MaskedDataset(self, mask)


    def key_by(self, key):
        '''
        Prepend the elements in this dataset with a key.

        The resulting dataset will consist of K,V tuples.

        :param key: callable(element)
            The transformation of the element which, when applied, provides the
            key value.

        Example::

            >>> import string
            >>> ctx.range(5).key_by(lambda i: string.ascii_lowercase[i]).collect()
            [('a', 0), ('b', 1), ('c', 2), ('d', 3), ('e', 4)]
        '''
        return self.map_partitions(lambda p: ((key(e), e) for e in p))


    def with_value(self, val):
        '''
        Create a dataset of K,V tuples with the elements of this dataset as K
        and V from the given value.

        :param val: callable(element) or obj
            If val is a callable, it will be applied to the elements of this
            dataset and the return values will be the values. If val is a plain
            object, it will be used as a constant value for each element.

        Example::

            >>> ctx.collection('abcdef').with_value(1).collect()
            [('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1), ('f', 1)]
        '''
        if not callable(val):
            return self.map_partitions(lambda p: ((e, val) for e in p))
        else:
            return self.map_partitions(lambda p: ((e, val(e)) for e in p))


    def keys(self):
        '''
        Pluck the keys from this dataset.

        Example:

            >>> ctx.collection([('a', 1), ('b', 2), ('c', 3)]).keys().collect()
            ['a', 'b', 'c']
        '''
        return self.pluck(0)


    def values(self):
        '''
        Pluck the values from this dataset.

        Example:

            >>> ctx.collection([('a', 1), ('b', 2), ('c', 3)]).keys().collect()
            [1, 2, 3]
        '''
        return self.pluck(1)


    def map_keys(self, func):
        '''
        Transform the keys of this dataset.

        :param func: callable(key)
            Transformation to apply to the keys
        '''
        return self.map_partitions(lambda p: ((func(k), v) for k, v in p))

    def map_values(self, func):
        '''
        Transform the values of this dataset.

        :param func: callable(value)
            Transformation to apply to the values
        '''
        return self.map_partitions(lambda p: ((k, func(v)) for k, v in p))

    def flatmap_values(self, func=None):
        '''
        :param func: callable(value) or None
            The callable which flattens the values of this dataset or None in
            order to use the values as iterables to flatten.
        '''
        return self.values().flatmap(func)


    def filter_bykey(self, func=None):
        '''
        Filter the dataset by testing the keys.

        :param func: callable(key)
            The test to apply to the keys. When positive, the key, value tuple
            will be retained.
        '''
        if func:
            return self.map_partitions(lambda p: (kv for kv in p if func(kv[0])))
        else:
            return self.map_partitions(lambda p: (kv for kv in p if kv[0]))


    def filter_byvalue(self, func=None):
        '''
        Filter the dataset by testing the values.

        :param func: callable(value)
            The test to apply to the values. When positive, the key, value tuple
            will be retained.
        '''
        if func:
            return self.map_partitions(lambda p: (kv for kv in p if func(kv[1])))
        else:
            return self.map_partitions(lambda p: (kv for kv in p if kv[1]))


    def first(self):
        '''
        Take the first element from this dataset.
        '''
        return next(self.itake(1))

    def take(self, num):
        '''
        Take the first num elements from this dataset.
        '''
        return list(self.itake(num))

    def itake(self, num):
        '''
        Take the first num elements from this dataset as iterator.
        '''
        # TODO don't use itake if first partition doesn't yield > 50% of num
        sliced = self.map_partitions(partial(take, num))
        results = sliced.icollect(eager=False)
        yield from islice(results, num)
        results.close()


    def nlargest(self, num, key=None):
        '''
        Take the num largest elements from this dataset.

        :param num: int
            The number of elements to take.
        :param key: callable(element) or None
            The (optional) key to apply when ordering elements.
        '''
        if num == 1:
            return self.max(key)
        return self._take_ordered(num, key, heapq.nlargest)


    def nsmallest(self, num, key=None):
        '''
        Take the num smallest elements from this dataset.

        :param num: int
            The number of elements to take.
        :param key: callable(element) or None
            The (optional) key to apply when ordering elements.
        '''
        if num == 1:
            return self.min(key)
        return self._take_ordered(num, key, heapq.nsmallest)


    def _take_ordered(self, num, key, taker):
        key = key_or_getter(key)
        func = partial(taker, num, key=key)
        return func(self.map_partitions(func).icollect())


    def histogram(self, bins=10):
        '''
        Compute the histogram of a data set.

        :param bins: int or sequence
            The bins to use in computing the histogram; either an int to indicate the number of
            bins between the minimum and maximum of this data set, or a sorted sequence of unique
            numbers to be used as edges of the bins.
        :return: A (np.array, np.array) tuple where the first array is the histogram and the
            second array the (edges of the) bins.

        The function behaves similarly to numpy.histogram, but only supports counts per bin (no
        weights or density/normalization). The resulting histogram and bins should match
        numpy.histogram very closely.

        Example:

            >>> ctx.collection([1, 2, 1]).histogram([0, 1, 2, 3])
            (array([0, 2, 1]), array([0, 1, 2, 3]))
            >>> ctx.range(4).histogram(np.arange(5))
            (array([1, 1, 1, 1]), array([0, 1, 2, 3, 4]))

            >>> ctx.range(4).histogram(5)
            (array([1, 1, 0, 1, 1]), array([ 0. ,  0.6,  1.2,  1.8,  2.4,  3. ]))
            >>> ctx.range(4).histogram()
            (array([1, 0, 0, 1, 0, 0, 1, 0, 0, 1]),
             array([ 0. ,  0.3,  0.6,  0.9,  1.2,  1.5,  1.8,  2.1,  2.4,  2.7,  3. ]))

            >>> dset = ctx.collection([1,2,1,3,2,4])
            >>> hist, bins = dset.histogram()
            >>> hist
            array([2, 0, 0, 2, 0, 0, 1, 0, 0, 1])
            >>> hist.sum() == dset.count()
            True

        '''
        if isinstance(bins, int):
            assert bins >= 1
            stats = self.stats()
            if stats.min == stats.max or bins == 1:
                return np.array([stats.count]), np.array([stats.min, stats.max])
            step = (stats.max - stats.min) / bins
            bins = [stats.min + i * step for i in range(bins)] + [stats.max]
        else:
            bins = sorted(set(bins))

        bins = np.array(bins)
        return self.map_partitions(lambda part: (np.histogram(list(part), bins)[0],)).reduce(add), bins


    def aggregate(self, local, comb=None):
        '''
        Collect an aggregate of this dataset, where the aggregate is determined
        by a local aggregation and a global combination.

        :param local: callable(partition)
            Function to apply on the partition iterable
        :param comb: callable
            Function to combine the results from local. If None, the local
            callable will be applied.
        '''
        try:
            parts = self.map_partitions(lambda p: (local(p),)).icollect()
            return (comb or local)(parts)
        except StopIteration:
            raise ValueError('dataset is empty')


    def combine(self, zero, merge_value, merge_combs):
        '''
        Aggregate the dataset by merging element-wise starting with a zero
        value and finally merge the intermediate results.
        
        :param zero: obj
            The object to merge values into.
        :param merge_value:
            The operation to merge an object into intermediate value (which
            initially is the zero value).
        :param merge_combs:
            The operation to pairwise combine the intermediate values into one
            final value.
            
        Example:
        
            >>> strings = ctx.range(1000*1000).map(lambda i: i%1000).map(str)
            >>> sorted(strings.combine(set(), lambda s, e: s.add(e) or s, lambda a, b: a|b)))
            ['0',
             '1',
             ...
             '998',
             '999']

        '''
        def _local(iterable):
            v = zero
            for e in iterable:
                merge_value(v, e)
            return v
        return self.aggregate(_local, partial(reduce, merge_combs))


    def reduce(self, reduction):
        '''
        Reduce the dataset into a final element by applying a pairwise
        reduction as with functools.reduce(...)
        
        :param reduction: The reduction to apply.
        
        Example:
        
            >>> ctx.range(100).reduce(lambda a,b: a+b)
            4950
        '''
        return self.aggregate(partial(reduce, reduction))


    def count(self):
        '''
        Count the elements in this dataset.
        '''
        return self.aggregate(iterable_size, sum)


    def sum(self):
        '''
        Sum the elements in this dataset.

        Example:

            >>> ctx.collection(['abc', 'def', 'ghi']).map(len).sum()
            9

        '''
        return self.aggregate(sum)


    def max(self, key=None):
        '''
        Take the largest element of this dataset.
        :param key: callable(element) or object
            The (optional) key to apply in comparing element. If key is an
            object, it is used to pluck from the element with the given to get
            the comparison key.

        Example:

            >>> ctx.range(10).max()
            9
            >>> ctx.range(10).with_value(1).max(0)
            (9, 1)
            >>> ctx.range(10).map(lambda i: dict(key=i, val=-i)).max('val')
            {'val': 0, 'key': 0}

        '''
        key = key_or_getter(key)
        return self.aggregate(partial(max, key=key) if key else max)


    def min(self, key=None):
        '''
        Take the smallest element of this dataset.
        :param key: callable(element) or object
            The (optional) key to apply in comparing element. If key is an
            object, it is used to pluck from the element with the given to get
            the comparison key.
        '''
        key = key_or_getter(key)
        return self.aggregate(partial(min, key=key) if key else min)


    def mean(self):
        '''
        Calculate the mean of this dataset.
        '''
        return self.stats().mean


    def stats(self):
        '''
        Calculate count, mean, min, max, variance, stdev, skew and kurtosis of
        this dataset.
        '''
        return self.aggregate(Stats, partial(reduce, add))


    def union(self, other):
        '''
        Union this dataset with another

        :param other: Dataset

        Example::

            >>> ctx.range(0, 5).union(ctx.range(5, 10)).collect()
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        '''
        return UnionDataset(self, other)


    def group_by(self, key, partitioner=None, pcount=None):
        '''
        Group the dataset by a given key function.

        :param key: callable(element) or obj
            The callable producing the key to group on or an index / indices
            for plucking the key from the elements. 
        :param partitioner: callable(element)
            A callable producing an integer which is used to determine to which
            partition the group is assigned.
        :param pcount:
            The number of partitions to group into.

        Example:

            >>> ctx.range(10).group_by(lambda i: i%2).collect()
            [(0, [0, 6, 8, 4, 2]), (1, [1, 3, 7, 9, 5])]

        '''
        key = key_or_getter(key)
        return (self.key_by(key)
                    .group_by_key(partitioner=partitioner, pcount=pcount)
                    .map_values(lambda val: pluck(1, val)))  # @UnusedVariable


    def group_by_key(self, partitioner=None, pcount=None):
        '''
        Group a K, V dataset by K.

        :param partitioner: callable
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to group into.
        '''
        def sort_and_group(partition):
            partition = sorted(partition, key=getter(0))
            if not partition:
                return ()
            key = partition[0][0]
            group = []
            for element in partition:
                if key == element[0]:
                    group.append(element)
                else:
                    yield key, group
                    group = [element]
                    key = element[0]
            yield key, group

        return (self.shuffle(key=getter(0), partitioner=partitioner, pcount=pcount)
                    .map_partitions(sort_and_group))


    def combine_by_key(self, create, merge_value, merge_combs, partitioner=None, pcount=None):
        '''
        Combine the values in a K, V1 dataset into a dataset of K, V2.

        :param create: callable(V1)
            A callable which returns the initial V2 for the value's key.
        :param merge_value: callable(V2, V1): V2
            A callable which merges a V1 into a V2.
        :param merge_combs: callable(V2, V2)
            A callable which merges two V2's.
        :param partitioner:
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to combine into.
        '''
        def _merge_vals(partition):
            items = {}
            for key, value in partition:
                if key in items:
                    items[key] = merge_value(items[key], value)
                else:
                    items[key] = create(value)
            return list(items.items())

        def _merge_combs(partition):
            items = {}
            for k, v in partition:
                if k in items:
                    items[k] = merge_combs(items[k], v)
                else:
                    items[k] = v
            return list(items.items())


        return self.map_partitions(_merge_vals) \
                   .shuffle(pcount, partitioner, key=getter(0)) \
                   .map_partitions(_merge_combs)


    def reduce_by_key(self, reduction, partitioner=None, pcount=None):
        '''
        Reduce the values of a K, V dataset.

        :param reduction: callable(v, v)
            The reduction to apply.
        :param partitioner:
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to reduce into.

        Example:

            >>> ctx.range(12).map(lambda i: (i%3, 1)).reduce_by_key(lambda a, b: a+b).collect()
            [(0, 4), (1, 4), (2, 4)]
        '''
        return self.combine_by_key(identity, reduction, reduction, pcount, partitioner)


    def join(self, other, key=None, partitioner=None, pcount=None):
        '''
        Join two datasets.

        :param other:
            The dataset to join with.
        :param key: callable(element) or object
            The callable which returns the join key or an object used as index
            to get the join key from the elements in the datasets to join.
        :param partitioner:
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to join into.

        Example::

            >>> ctx.range(0, 5).key_by(lambda i: i%2).join(ctx.range(5, 10).key_by(lambda i: i%2)).collect()
            [(0, [(0, 8), (0, 6), (2, 8), (2, 6), (4, 8), (4, 6)]),
             (1, [(1, 5), (1, 9), (1, 7), (3, 5), (3, 9), (3, 7)])]
        '''
        key = key_or_getter(key)

        if key:
            # add a key to keep left from right
            # also apply the key function
            left = self.map_partitions(lambda p: ((key(e), (0, e)) for e in p))
            right = other.map_partitions(lambda p: ((key(e), (1, e)) for e in p))
        else:
            # add a key to keep left from right
            left = self.map_values(lambda v: (0, v))
            right = other.map_values(lambda v: (1, v))

        both = left.union(right)
        shuffled = both.group_by_key(partitioner=partitioner, pcount=pcount)

        def local_join(group):
            key, group = group
            left, right = [], []
            left_append, right_append = left.append, right.append
            for (idx, value) in pluck(1, group):
                if idx:
                    right_append(value)
                else:
                    left_append(value)
            if left and right:
                return key, list(product(left, right))

        joined = shuffled.map(local_join)
        return joined.filter()


    def distinct(self, pcount=None):
        '''
        Select the distinct elements from this dataset.

        :param pcount:
            The number of partitions to shuffle into.

        Example:

            >>> sorted(ctx.range(10).map(lambda i: i%2).distinct().collect())
            [0, 1]
        '''
        shuffle = self.shuffle(pcount, bucket=SetBucket, comb=set)
        return shuffle.map_partitions(set)


    def count_distinct(self):
        '''
        Count the distinct elements in this Dataset.
        '''
        return self.distinct().count()


    def count_distinct_approx(self, error_rate=.05):
        '''
        Approximate the count of distinct elements in this Dataset through
        the hyperloglog++ algorithm based on https://github.com/svpcom/hyperloglog.
        
        :param error_rate: float
            The absolute error / cardinality
        '''
        return self.map(_as_bytes).aggregate(
            lambda i: HyperLogLog(error_rate).add_all(i),
            lambda hlls: HyperLogLog(error_rate).merge(*hlls)
        ).card()


    def count_by_value(self):
        '''
        Count the occurrence of each distinct value in the data set.
        '''
        return self.aggregate(Counter, lambda counters: sum(counters, Counter()))



    def sort(self, key=identity, reverse=False, pcount=None):
        '''
        Sort the elements in this dataset.

        :param key: callable or obj
            A callable which returns the sort key or an object which is the
            index in the elements for getting the sort key.
        :param reverse: bool
            If True perform a sort in descending order, or False to sort in
            ascending order. 
        :param pcount:
            Optionally the number of partitions to sort into.

        Example:

            >>> ''.join(ctx.collection('asdfzxcvqwer').sort().collect())
            'acdefqrsvwxz'

            >>> ctx.range(5).map(lambda i: dict(a=i-2, b=i)).sort(key='a').collect()
            [{'b': 0, 'a': -2}, {'b': 1, 'a': -1}, {'b': 2, 'a': 0}, {'b': 3, 'a': 1}, {'b': 4, 'a': 2}]

            >>> ctx.range(5).key_by(lambda i: i-2).sort(key=1).sort().collect()
            [(-2, 0), (-1, 1), (0, 2), (1, 3), (2, 4)]
        '''
        key = key_or_getter(key)

        pcount = pcount or self.ctx.default_pcount
        # TODO if sort into 1 partition

        dset_size = self.count()
        if dset_size == 0:
            return self

        # sample to find a good distribution over buckets
        fraction = min(pcount * 20. / dset_size, 1.)
        samples = self.sample(fraction).collect()
        # apply the key function if any
        if key:
            samples = map(key, samples)
        # sort the samples to function as boundaries
        samples = sorted(set(samples), reverse=reverse)
        # take pcount - 1 points evenly spaced from the samples as boundaries
        boundaries = [samples[len(samples) * (i + 1) // pcount] for i in range(pcount - 1)]
        # and use that in the range partitioner to shuffle
        partitioner = RangePartitioner(boundaries, reverse)
        shuffled = self.shuffle(pcount, partitioner=partitioner, key=key)
        # finally sort within the partition
        return shuffled.map_partitions(partial(sorted, key=key, reverse=reverse))


    def shuffle(self, pcount=None, partitioner=None, bucket=None, key=None, comb=None):
        shuffle = self._shuffle(pcount, partitioner, bucket, key, comb)
        return ShuffleReadingDataset(self.ctx, shuffle)

    def _shuffle(self, pcount=None, partitioner=None, bucket=None, key=None, comb=None):
        key = key_or_getter(key)
        return ShuffleWritingDataset(self.ctx, self, pcount, partitioner, bucket, key, comb)


    def zip(self, other):
        # TODO what if some partition is shorter/longer than another?
        return self.zip_partitions(other, zip)

    def zip_partitions(self, other, comb):
        from .zip import ZippedDataset
        return ZippedDataset(self, other, comb=comb)


    def sample(self, fraction, with_replacement=False, seed=None):
        if fraction == 0.0:
            return self.ctx.range(0)
        elif fraction == 1.0:
            return self

        assert 0 < fraction < 1

        import numpy as np
        rng = np.random.RandomState(seed)

        sampling = sample_with_replacement if with_replacement else sample_without_replacement
        return self.map_partitions(partial(sampling, rng, fraction))

    # TODO implement stratified sampling

    def take_sample(self, num, with_replacement=False, seed=None):
        '''
        based on https://github.com/apache/spark/blob/master/python/pyspark/rdd.py#L425
        '''
        num = int(num)
        assert num >= 0
        if num == 0:
            return []

        count = self.count()
        if count == 0:
            return []

        import numpy as np
        rng = np.random.RandomState(seed)

        if (not with_replacement) and num >= count:
            return rng.shuffle(self.collect())

        fraction = float(num) / count
        if with_replacement:
            num_stdev = 9 if (num < 12) else 5
            fraction = fraction + num_stdev * sqrt(fraction / count)
        else:
            delta = 0.00005
            gamma = -log(delta) / count
            fraction = min(1, fraction + gamma + sqrt(gamma * gamma + 2 * gamma * fraction))

        samples = self.sample(fraction, with_replacement, seed).collect()

        while len(samples) < num:
            seed = rng.randint(0, np.iinfo(np.uint32).max)
            samples = self.sample(fraction, with_replacement, seed).collect()

        rng.shuffle(samples)
        return samples[0:num]



    def collect(self, parts=False):
        return list(self.icollect(parts=parts))


    def collect_as_map(self, parts=False):
        if parts:
            return list(map(dict, self.icollect(parts=True)))
        else:
            return dict(self.icollect())


    def collect_as_set(self):
        return set(self.icollect())


    def collect_as_pickles(self, directory=None):
        if not directory:
            directory = os.getcwd()
        pickled = self.map_partitions(lambda p: (p if is_stable_iterable(p) else list(p),)).map(pickle.dumps)
        for idx, part in enumerate(pickled.icollect()):
            with open(os.path.join(directory, '%s-%s.p' % (self.id, idx)), 'wb') as f:
                f.write(part)


    def icollect(self, eager=True, parts=False):
        result = self._execute(eager)
        result = filter(lambda p: p is not None, result)  # filter out empty parts
        if not parts:
            result = chain.from_iterable(result)  # chain the parts into a big iterable
        yield from result


    def foreach(self, func):
        for element in self.icollect():
            func(element)


    def execute(self):
        for _ in self._execute():
            pass

    def _execute(self, eager=True):
        yield from self.ctx.execute(self._schedule(), eager=eager)

    def _schedule(self):
        return schedule_job(self)


    def prefer_workers(self, fltr):
        return self._with('_worker_preference', fltr)

    def allow_workers(self, fltr):
        return self._with('_worker_filter', fltr)

    def require_local_workers(self):
        return self.allow_workers(_filter_local_workers)

    def allow_all_workers(self):
        return self.allow_workers(None)


    def cache(self, cached=True):
        assert self.ctx.node.node_type == 'driver'
        # set mark
        self._cache = cached
        # cleanup on 'uncache'
        if not cached:
            # issue uncache tasks
            cache_loc_names = set(self._cache_locs.values())
            tasks = [
                worker.uncache_dset.with_timeout(1)(self.id)
                for worker in self.ctx.workers
                if worker.name in cache_loc_names]
            # wait for them to finish
            for task in tasks:
                with catch(Exception):
                    task.result()
            # clear cache locations
            self._cache_locs = {}

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
            if not is_stable_iterable(data):
                data = list(data)
            dset_cache = worker.dset_cache.setdefault(self.dset.id, {})
            dset_cache[self.idx] = data

        # return data
        return data


    @property
    def cache_loc(self):
        return self.dset._cache_locs.get(self.idx, None)


    @abc.abstractmethod
    def _materialize(self, ctx):
        pass


    def preferred_workers(self, workers):
        if self.cache_loc:
            return [worker for worker in workers if worker.name == self.cache_loc]
        else:
            if self.dset._worker_preference:
                return self.dset._worker_preference(workers)
            else:
                return self._preferred_workers(workers)


    def _preferred_workers(self, workers):
        if self.src:
            return self.src.preferred_workers(workers)
        else:
            return None


    def allowed_workers(self, workers):
        if self.dset._worker_filter:
            return self.dset._worker_filter(workers)
        else:
            return self._allowed_workers(workers)


    def _allowed_workers(self, workers):
        if self.src:
            return self.src.allowed_workers(workers)
        else:
            return workers


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



class ListBucket(list):
    add = list.append


class SetBucket(set):
    extend = set.update


class SortedListBucket(sortedcontainers.sortedlist.SortedList):
    def extend(self, iterable):
        self.update(iterable)
        return self



class RangePartitioner():
    def __init__(self, boundaries, reverse=False):
        self.boundaries = boundaries
        self.reverse = reverse

    def __call__(self, value):
        boundaries = self.boundaries
        boundary = bisect_left(boundaries, value)
        return len(boundaries) - boundary if self.reverse else boundary



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
        bucket_count = len(buckets)

        if bucket_count:
            key = self.dset.key
            partitioner = self.dset.partitioner

            if key:
                def select_bucket(element):
                    return partitioner(key(element))
            else:
                select_bucket = partitioner

            for element in self.src.materialize(ctx):
                buckets[select_bucket(element) % bucket_count].add(element)

            if self.dset.comb:
                for key, bucket in enumerate(buckets):
                    if bucket:
                        buckets[key] = self.dset.bucket(self.dset.comb(bucket))
        else:
            data = self.src.materialize(ctx)
            if self.dset.comb:
                data = self.dset.comb(data)
            buckets[0].extend(data)



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
        bucket = self.dset.ctx.node.get_bucket(None, self.dset.src.id, self.idx)
        if bucket:
            yield from bucket

        futures = [
            worker.get_bucket(self.dset.src.id, self.idx)
            for worker in self.dset.ctx.workers
        ]

        for future in futures:
            # TODO timeout and reschedule
            yield from future.result()

        del futures


class TransformingDataset(Dataset):
    def __init__(self, ctx, src, transformation):
        super().__init__(ctx, src)
        self.transformation = transformation
        self._transformation = cycloudpickle.dumps(self.transformation)  # @UndefinedVariable

    def parts(self):
        return [
            TransformingPartition(self, i, part)
            for i, part in enumerate(self.src.parts())
        ]

    def __getstate__(self):
        state = copy(self.__dict__)
        del state['transformation']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.transformation = cycloudpickle.loads(self._transformation)  # @UndefinedVariable


class TransformingPartition(Partition):
    def _materialize(self, ctx):
        data = self.src.materialize(ctx)
        return self.dset.transformation(self.src, data if data is not None else ())
