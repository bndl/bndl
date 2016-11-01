from collections import Counter, deque, defaultdict, Iterable, Sized, OrderedDict
from functools import partial, total_ordering, reduce
from itertools import islice, product, chain, starmap, groupby
from math import sqrt, log, ceil
from operator import add
import concurrent.futures
import gzip
import heapq
import io
import json
import logging
import os
import pickle
import shlex
import struct
import subprocess
import threading
import weakref

from cytoolz.functoolz import compose
from cytoolz.itertoolz import pluck, take
import numpy as np

from ..compute import cache
from ..execute import TaskCancelled
from ..execute.job import RemoteTask, Job
from ..execute.worker import task_context
from ..rmi import InvocationException
from ..util import serialize, cycloudpickle as cloudpickle, strings
from ..util.collection import is_stable_iterable, ensure_collection
from ..util.exceptions import catch
from ..util.funcs import identity, getter, key_or_getter
from ..util.hash import portable_hash
from ..util.hyperloglog import HyperLogLog
from .explain import get_callsite, flatten_dset
from .stats import iterable_size, Stats, sample_with_replacement, sample_without_replacement


logger = logging.getLogger(__name__)


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


class Dataset(object):
    cleanup = None
    sync_required = False

    def __init__(self, ctx, src=None, dset_id=None):
        self.ctx = ctx
        self.src = src
        self.id = dset_id or strings.random(8)
        self._cache_provider = False
        self._cache_locs = {}
        self._worker_preference = None
        self._worker_filter = None
        self.name, self.get_callsite = get_callsite(type(self))


    def parts(self):
        return ()


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
        Transform the partitions - with the source partition object as argument - of
        this dataset.

        :param func: callable(partition, iterator)
            The transformation to apply on the partition object and the iterator
            over the partition's elements.
        '''
        return TransformingDataset(self, func)


    def pipe(self, command, reader=io.IOBase.readlines, writer=io.IOBase.writelines, **opts):
        if isinstance(command, str):
            command = shlex.split(command)

        def pipe_partition(partition):
            opts['stdin'] = subprocess.PIPE
            opts['stdout'] = subprocess.PIPE
            proc = subprocess.Popen(command, **opts)

            failure = [None]
            def write_partition(failure):
                try:
                    writer(proc.stdin, partition)
                except Exception as exc:
                    failure[0] = exc
                finally:
                    proc.stdin.close()

            writer_thread = threading.Thread(target=write_partition, args=[failure])
            writer_thread.start()
            yield from reader(proc.stdout)
            writer_thread.join()

            failure = failure[0]
            if failure:
                raise failure

        return self.map_partitions(pipe_partition)


    def glom(self):
        '''
        Transforms each partition into a partition with one element being the
        contents of the partition as a 'stable iterable' (e.g. a list).
        
        See the bndl.util.collection.is_stable_iterable function for details on
        what constitutes a stable iterable.
        
        Example::
        
            >>> ctx.range(10, pcount=4).map_partitions(list).glom().collect()
            [[0, 1], [2, 3, 4], [5, 6], [7, 8, 9]]
        '''
        return self.map_partitions(lambda p: (ensure_collection(p),))


    def concat(self, sep):
        if isinstance(sep, str):
            def f(part):
                out = io.StringIO()
                write = out.write
                for e in part:
                    write(e)
                    write(sep)
                return (out.getvalue(),)
        elif isinstance(sep, (bytes, bytearray)):
            def f(part):
                buffer = bytearray()
                extend = buffer.extend
                for e in part:
                    extend(e)
                    extend(sep)
                return (buffer,)
        else:
            raise ValueError('sep must be str, bytes or bytearray, not %s' % type(sep))
        return self.map_partitions(f)


    def parse_csv(self, sample=None, **kwargs):
        import pandas as pd
        from bndl.compute import dataframes

        def as_df(part):
            dfs = (pd.read_csv(io.StringIO(e), **kwargs) for e in part)
            return pd.concat(list(dfs))
        dsets = self.map_partitions(as_df)

        if sample is None:
            columns = kwargs.pop('names', None)
            if columns:
                return dataframes.DistributedDataFrame(dsets, [None], columns)
            else:
                sample = pd.read_csv(io.StringIO(self.first()), **kwargs)

        return dataframes.DistributedDataFrame.from_sample(dsets, sample)


    def as_dataframe(self, *args, **kwargs):
        from bndl.compute import dataframes
        return dataframes.DistributedDataFrame.from_dataset(self, *args, **kwargs)


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

        Example:

            >>> ctx.collection('abcdef').with_value(1).collect()
            [('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1), ('f', 1)]
        '''
        if not callable(val):
            return self.map_partitions(lambda p: ((e, val) for e in p))
        else:
            return self.map_partitions(lambda p: ((e, val(e)) for e in p))


    def key_by_id(self):
        '''
        Key the elements of this data set with a unique integer id.
        
        Example:
        
            >>> ctx.collection(['a', 'b', 'c', 'd', 'e'], pcount=2).key_by_id().collect()
            [(0, 'a'), (2, 'b'), (4, 'c'), (1, 'd'), (3, 'e')]
        '''
        n = len(self.parts())
        def with_id(idx, part):
            return ((idx + i * n, e) for i, e in enumerate(part))
        return self.map_partitions_with_index(with_id)


    def key_by_idx(self):
        '''
        Key the elements of this data set with their index.
        
        This operation starts a job when the data set contains more than 1
        partition to calculate offsets for each of the partitions. Use
        key_by_id or cache the data set to speed up processing.
        
        Example:
        
            >>> ctx.collection(['a', 'b', 'c', 'd', 'e']).key_by_idx().collect()
            [(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'e')]
        '''
        offsets = [0]
        if len(self.parts()) > 1:
            for size in self.map_partitions(lambda p: (iterable_size(p),)).collect():
                offsets.append(offsets[-1] + size)
        def with_idx(idx, part):
            return enumerate(part, offsets[idx])
        return self.map_partitions_with_index(with_idx)


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


    def nsmallest_by_key(self, num, key=None, **shuffle_opts):
        '''Shuffle and keep only the num smallest elements for each key.'''
        return self._take_ordered_by_key(num, key, heapq.nsmallest, **shuffle_opts)


    def nlargest_by_key(self, num, key=None, **shuffle_opts):
        '''Shuffle and keep only the num largest elements for each key.'''
        return self._take_ordered_by_key(num, key, heapq.nlargest, **shuffle_opts)


    def _take_ordered_by_key(self, num, key, taker, **shuffle_opts):
        def local(values):
            return taker(num, values, key=key)
        def comb(iterables):
            return taker(num, chain.from_iterable(iterables), key=key)
        return self.aggregate_by_key(local, comb, **shuffle_opts)


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
            return reduce(merge_value, iterable, zero)
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


    def tree_aggregate(self, local, comb=None, depth=2, scale=None, **shuffle_opts):
        '''
        Tree-wise aggregation by first applying local on each partition and
        subsequently shuffling the data across workers in depth rounds and for
        each round aggregating the data by applying comb.

        :param local: func(iterable)
            The aggregation function to apply to each partition.
        :param comb:
            The function to apply in order to combine aggregated partitions.
        :param depth:
            The number of iterations to apply the aggregation in.
        :param scale: int or None (default)
            The factor by which to reduce the partition count in each round. If
            None, the step is chosen such that each reduction of intermediary
            results is roughly of the same size (the branching factor in the
            tree is the same across the entire tree).
        '''
        if depth < 2:
            return self.aggregate(local, comb)

        if not comb:
            comb = local

        if not scale:
            pcount = len(self.parts())
            scale = max(int(ceil(pow(pcount, 1.0 / depth))), 2)

        agg = self.map_partitions_with_index(lambda idx, p: [(idx % pcount, local(p))])

        for _ in range(depth):
            agg = agg.group_by_key(pcount=pcount, **shuffle_opts).map_values(lambda v: comb(pluck(1, v)))
            pcount //= scale
            if pcount < scale:
                break
            agg = agg.map_keys(lambda idx, : idx % pcount)

        try:
            return comb(agg.values().icollect())
        except StopIteration:
            raise ValueError('dataset is empty')


    def tree_combine(self, zero, merge_value, merge_combs, **kwargs):
        '''
        Tree-wise version of Dataset.combine. See Dataset.tree_aggregate for details.
        '''
        def _local(iterable):
            return reduce(merge_value, iterable, zero)
        return self.tree_aggregate(_local, partial(reduce, merge_combs), **kwargs)


    def tree_reduce(self, reduction, **kwargs):
        '''
        Tree-wise version of Dataset.reduce. See Dataset.tree_aggregate for details.
        '''
        return self.tree_aggregate(partial(reduce, reduction), **kwargs)


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


    def union(self, other, *others):
        '''
        Union this dataset with another

        :param other: Dataset

        Example::

            >>> ctx.range(0, 5).union(ctx.range(5, 10)).collect()
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        '''
        return UnionDataset((self, other) + others)


    def group_by(self, key, partitioner=None, pcount=None, **shuffle_opts):
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
                    .group_by_key(partitioner=partitioner, pcount=pcount, **shuffle_opts)
                    .map_values(tuple))


    def group_by_key(self, partitioner=None, pcount=None, **shuffle_opts):
        '''
        Group a K, V dataset by K.

        :param partitioner: callable
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to group into.
        '''
        def strip_key(key, value):
            return key, pluck(1, value)
        return self._group_by_key(partitioner, pcount, **shuffle_opts).starmap(strip_key).map_values(tuple)


    def _group_by_key(self, partitioner=None, pcount=None, **shuffle_opts):
        def _group_by_key(partition):
            return groupby(partition, key=getter(0))
        return (self.shuffle(pcount, partitioner, key=getter(0), **shuffle_opts)
                    .map_partitions(_group_by_key))


    def aggregate_by_key(self, local, comb=None, partitioner=None, pcount=None, **shuffle_opts):
        '''
        Aggregate the values in a K, V1 dataset into a dataset of K, V2.

        :param local: callable(iterable[V1])
            A callable which returns the aggregation V2 for the values of a key.
        :param comb: callable(iterable[V2]): V2 (optional)
            A callable which performs a per key aggregation of the V2's
            generated by local. Defaults to local.
        :param partitioner:
            The (optional) partitioner to apply.
        :param pcount:
            The number of partitions to combine into.
        '''
        if not comb:
            comb = local

        def local_aggregation(partition):
            for key, group in groupby(partition, key=getter(0)):
                yield key, local(pluck(1, group))

        def merge_aggregations(partition):
            for key, group in groupby(partition, key=getter(0)):
                yield key, comb(pluck(1, group))

        return (self.shuffle(pcount, partitioner, key=getter(0),
                             comb=local_aggregation, **shuffle_opts)
                    .map_partitions(merge_aggregations))


    def combine_by_key(self, create, merge_value, merge_combs,
                       partitioner=None, pcount=None, **shuffle_opts):
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
        def merge_values(values):
            values = iter(values)
            merged = create(next(values))
            for value in values:
                merged = merge_value(merged, value)
            return merged

        def merge_combined(combined):
            combined = iter(combined)
            merged = next(combined)
            for value in combined:
                merged = merge_combs(merged, value)
            return merged

        return self.aggregate_by_key(merge_values, merge_combined, partitioner,
                                     pcount, **shuffle_opts)


    def reduce_by_key(self, reduction, partitioner=None, pcount=None, **shuffle_opts):
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
        return self.combine_by_key(identity, reduction, reduction, partitioner, pcount, **shuffle_opts)


    def _cogroup(self, other, *others, key=None, partitioner=None, pcount=None, **shuffle_opts):
        key = key_or_getter(key)

        rdds = []
        for idx, rdd in enumerate((self, other) + others):
            if key is None:
                rdds.append(rdd.map_values(lambda v: (idx, v)))
            else:
                rdds.append(rdd.map_partitions(lambda p, idx=idx: ((key(e), (idx, e)) for e in p)))

        return UnionDataset(rdds)._group_by_key(partitioner, pcount, **shuffle_opts)


    def cogroup(self, other, *others, key=None, partitioner=None, pcount=None, **shuffle_opts):
        num_rdds = 2 + len(others)

        def local_cogroup(group):
            key, group = group

            buckets = [[] for _ in range(num_rdds)]
            for idx, value in pluck(1, group):
                buckets[idx].append(value)
            return key, buckets

        return (self._cogroup(other, *others, key=key, partitioner=partitioner, pcount=pcount, **shuffle_opts)
                    .map(local_cogroup))


    def join(self, other, key=None, partitioner=None, pcount=None, **shuffle_opts):
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
        def local_join(group):
            key, groups = group
            if all(groups):
                yield key, tuple(product(*groups))

        return (self.cogroup(other, key=key, partitioner=partitioner, pcount=pcount, **shuffle_opts)
                    .flatmap(local_join))


    def product(self, other, *others):
        return CartesianProductDataset((self, other) + others)



    def distinct(self, pcount=None, key=None, **shuffle_opts):
        '''
        Select the distinct elements from this dataset.

        :param pcount:
            The number of partitions to shuffle into.

        Example:

            >>> sorted(ctx.range(10).map(lambda i: i%2).distinct().collect())
            [0, 1]
        '''
        key = key_or_getter(key)

        if key is not None:
            from .shuffle import DictBucket
            bucket = DictBucket
        else:
            from .shuffle import SetBucket
            bucket = SetBucket

        shuffle = self.shuffle(pcount, bucket=bucket, key=key, comb=sorted, sort=True, **shuffle_opts)

        def select_distinct(partition):
            for _, group in groupby(partition, key=key):
                yield next(group)
        return shuffle.map_partitions(select_distinct)


    def count_distinct(self, pcount=None, **shuffle_opts):
        '''
        Count the distinct elements in this Dataset.
        '''
        return self.distinct(pcount, **shuffle_opts).count()


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


    def count_by_value(self, depth=1, **shuffle_opts):
        '''
        Count the occurrence of each distinct value in the data set.
        '''
        return self.tree_aggregate(Counter, lambda counters: sum(counters, Counter()),
                                   depth=depth, **shuffle_opts)



    def sort(self, key=None, reverse=False, pcount=None, hd_distribution=False, **shuffle_opts):
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
        from bndl.compute.shuffle import RangePartitioner
        key = key_or_getter(key)

        if pcount is None:
            pcount = len(self.parts())

        if pcount == 1:
            return self.shuffle(pcount, key=key, **shuffle_opts)

        point_count = pcount * 20

        if hd_distribution:
            dset_size = self.count()
            if dset_size == 0:
                return self
            # sample to find a good distribution over buckets
            fraction = min(pcount * 20. / dset_size, 1.)
            samples = self.sample(fraction).collect_as_set()
        else:
            rng = np.random.RandomState()
            def sampler(partition):
                fraction = 0.1

                if isinstance(partition, Sized):
                    if len(partition) <= point_count:
                        return partition
                    else:
                        fraction = point_count / len(partition)
                        samples = sample_without_replacement(rng, fraction, partition)
                else:
                    samples1 = list(islice(partition, point_count))
                    samples2 = sample_without_replacement(rng, fraction, partition)
                    short = point_count - len(samples2)
                    if short > 0:
                        if len(samples1) < short:
                            samples = samples1 + samples2
                        else:
                            rng.shuffle(samples1)
                            samples = samples1[:short] + samples2
                    elif short < 0:
                        rng.shuffle(samples1)
                        samples = samples1[len(samples1) * fraction] + samples2
                if len(samples) > point_count:
                    rng.shuffle(samples)
                    return samples[:point_count]
                else:
                    return samples
            samples = self.map_partitions(sampler).collect_as_set()

        assert samples

        # apply the key function if any
        if key:
            samples = set(map(key, samples))
        # sort the samples to function as boundaries
        samples = sorted(samples, reverse=reverse)
        # take pcount - 1 points evenly spaced from the samples as boundaries
        boundaries = [samples[len(samples) * (i + 1) // pcount] for i in range(pcount - 1)]
        # and use that in the range partitioner to shuffle
        partitioner = RangePartitioner(boundaries, reverse)
        return self.shuffle(pcount, partitioner=partitioner, key=key, **shuffle_opts)


    def shuffle(self, pcount=None, partitioner=None, bucket=None, key=None, comb=None, sort=None, **opts):
        key = key_or_getter(key)
        from .shuffle import ShuffleReadingDataset, ShuffleWritingDataset, ListBucket
        if bucket is None and sort == False:
            bucket = ListBucket
        shuffle = ShuffleWritingDataset(self, pcount, partitioner, bucket, key, comb, **opts)
        return ShuffleReadingDataset(shuffle, sort)


    def zip(self, other):
        '''
        Zip the elements of another data set with the elements of this data set.

        :param other: bndl.compute.dataset.Dataset
            The other data set to zip with.

        Example:

            >>> ctx.range(0,10).zip(ctx.range(10,20)).collect()
            [(0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16), (7, 17), (8, 18), (9, 19)]
        '''
        # TODO what if some partition is shorter/longer than another?
        return self.zip_partitions(other, zip)


    def zip_partitions(self, other, comb):
        '''
        Zip the partitions of another data set with the partitions of this data set.

        :param other: bndl.compute.dataset.Dataset
            The other data set to zip the partitions of with the partitions of this data set.
        :param comb: func(iterable, iterable)
            The function which combines the data of the partitions from this
            and the other data sets.

        Example:

            >>> ctx.range(0,10).zip_partitions(ctx.range(10,20), lambda a, b: zip(a,b)).collect()
            [(0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16), (7, 17), (8, 18), (9, 19)]
        '''
        from .zip import ZippedDataset
        return ZippedDataset(self, other, comb=comb)


    def sample(self, fraction, with_replacement=False, seed=None):
        if fraction == 0.0:
            return self.ctx.range(0)
        elif fraction == 1.0:
            return self

        assert 0 < fraction < 1

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



    def collect(self, parts=False, ordered=True):
        return list(self.icollect(parts, ordered))


    def collect_as_map(self, parts=False):
        dicts = self.map_partitions(dict)
        if parts:
            return dicts.collect(True, True)
        else:
            combined = {}
            for d in dicts.icollect(True, False):
                combined.update(d)
            return combined


    def collect_as_set(self):
        s = set()
        for part in self.map_partitions(set).icollect(True, False):
            s.update(part)
        return s


    def collect_as_pickles(self, directory=None, compress=None):
        '''
        Collect each partition as a pickle file into directory
        '''
        self.glom().map(pickle.dumps).collect_as_files(directory, '.p', 'b', compress)


    def collect_as_json(self, directory=None, compress=None):
        '''
        Collect each partition as a line separated json file into directory.
        '''
        self.map(json.dumps).concat(os.linesep).collect_as_files(directory, '.json', 't', compress)


    def collect_as_files(self, directory=None, ext='', mode='b', compress=None):
        '''
        Collect each element in this data set into a file into directory.
        
        :param directory: str
            The directory to save this data set to.
        :param ext:
            The extenion of the files.
        :param compress: None or 'gzip'
            Whether to compress.
        '''
        if not directory:
            directory = os.getcwd()
        if mode not in ('t', 'b'):
            raise ValueError('mode should be t(ext) or b(inary)')
        data = self
        # compress if necessary
        if compress == 'gzip':
            ext += '.gz'
            if mode == 't':
                data = data.map(lambda e: e.encode())
            # compress concatenation of partition, not just each element
            mode = 'b'
            data = data.concat(b'').map(gzip.compress)
        elif compress is not None:
            raise ValueError('Only gzip compression is supported')
        # add an index to the partitions (for in the filename)
        with_idx = data.map_partitions_with_index(lambda idx, part: (idx, ensure_collection(part)))
        # save each partition to a file
        for idx, part in with_idx.icollect(ordered=False, parts=True):
            with open(os.path.join(directory, '%s%s' % (idx, ext)), 'w' + mode) as f:
                f.writelines(part)


    def execute(self):
        def consume_iterable(i):
            if not is_stable_iterable(i):
                sum(1 for _ in i)
        for _ in self.map_partitions(consume_iterable)._execute(ordered=False):
            pass


    def icollect(self, parts=False, ordered=True):
        result = self._execute(ordered)
        result = filter(lambda p: p is not None, result)  # filter out empty parts
        if not parts:
            result = chain.from_iterable(result)  # chain the parts into a big iterable
        yield from result


    def first(self):
        '''
        Take the first element from this dataset.
        '''
        taker = self.itake(1)
        first = next(taker)
        taker.close()
        return first


    def take(self, num):
        '''
        Take the first num elements from this dataset.
        '''
        return list(self.itake(num))


    def itake(self, num):
        '''
        Take the first num elements from this dataset as iterator.
        '''
        assert self.ctx.running, 'context of dataset is not running'
        remaining = num
        sliced = self.map_partitions(partial(take, remaining)).itake_parts()
        try:
            for part in sliced:
                if part is None:
                    continue
                for e in islice(part, remaining):
                    yield e
                    remaining -= 1
                if not remaining:
                    break
        finally:
            sliced.close()


    def itake_parts(self):
        mask = slice(0, 1)
        pcount = len(self.parts())
        while mask.start < pcount:
            masked = self.mask_partitions(lambda parts: parts[mask])
            done = self.ctx.execute(masked._schedule())
            try:
                for task in done:
                    yield task.result()
                mask = slice(mask.stop, mask.stop * 2 + 1)
            finally:
                done.close()


    def _execute(self, ordered=True):
        assert self.ctx.running, 'context of dataset is not running'
        job = self._schedule()
        done = self.ctx.execute(job, order_results=ordered)
        for task in done:
            yield task.result()


    def _tasks(self):
        tasks = OrderedDict()
        group = 1
        groups = 1

        def generate_task(part):
            nonlocal group, groups

            task = tasks.get(part.id)
            if task:
                return task

            stack = deque()
            task = ComputePartitionTask(part, group)

            if isinstance(part.src, Iterable):
                stack.extend(part.src)
            elif part.src is not None:
                stack.append(part.src)

            while stack:
                p = stack.popleft()
                if p.dset.sync_required:
                    group += 1
                    groups = max(groups, group)
                    dependency = generate_task(p)
                    task.dependencies.append(dependency)
                    dependency.dependents.append(task)
                    group -= 1
                else:
                    if isinstance(p.src, Iterable):
                        stack.extend(p.src)
                    elif p.src is not None:
                        stack.append(p.src)

            tasks[part.id] = task
            return task

        for part in self.parts():
            generate_task(part)


        taskslist = []
        for task in tasks.values():
            task.group = groups - task.group + 1
            taskslist.append(task)
        return taskslist


    def _schedule(self):
        name, desc = get_callsite()
        job = Job(self.ctx, self._tasks(), name, desc)

        cleanups = []
        for dset in flatten_dset(self):
            if dset.cleanup:
                cleanups.append(dset.cleanup)

        if cleanups:
            def cleanup(job):
                if job.stopped:
                    for cleanup in cleanups:
                        cleanup(job)
            job.add_listener(cleanup)

        return job


    def cache(self, location='memory', serialization=None, compression=None, provider=None):
        assert self.ctx.node.node_type == 'driver'
        if not location:
            self.uncache()
        else:
            assert not self._cache_provider
            if location == 'disk' and not serialization:
                serialization = 'pickle'
            self._cache_provider = cache.CacheProvider(location, serialization, compression)
        return self


    @property
    def cached(self):
        return bool(self._cache_provider)


    def uncache(self, block=False, timeout=None):
        # issue uncache tasks
        def clear(worker, provider=self._cache_provider, dset_id=self.id):
            provider.clear(dset_id)

        tasks = [
            worker.run_task
            for worker in self.ctx.workers
            if worker.name in set(self._cache_locs.values())]

        if timeout:
            tasks = [task.with_timeout(timeout) for task in tasks]

        tasks = [task(clear) for task in tasks]

        if block:
            # wait for them to finish
            for task in tasks:
                try:
                    task.result()
                except concurrent.futures.TimeoutError:
                    pass
                except Exception:
                    logger.warning('error while uncaching %s', self, exc_info=True)

        # clear cache locations
        self._cache_locs = {}
        self._cache_provider = None
        return self


    def __del__(self):
        if getattr(self, '_cache_provider', None):
            node = None
            with catch(RuntimeError):
                node = self.ctx.node
            if node and node.node_type == 'driver':
                self.uncache()


    def __hash__(self):
        return hash(self.id)


    def __eq__(self, other):
        return self.id == other.id


    def _with(self, *args, **kwargs):
        clone = type(self).__new__(type(self))
        clone.__dict__ = dict(self.__dict__)
        if args:
            for attribute, value in zip(args[0::2], args[1::2]):
                setattr(clone, attribute, value)
        clone.__dict__.update(kwargs)
        clone.id = strings.random(8)
        return clone


    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)


    def __str__(self):
        return self.name


FORBIDDEN = -1
NON_LOCAL = 0
LOCAL = 1
NODE_LOCAL = 3
PROCESS_LOCAL = 4


@total_ordering
class Partition(object):
    def __init__(self, dset, idx, src=None):
        self.dset = weakref.proxy(dset)
        self.idx = idx
        self.src = src
        self.id = (dset.id, idx)


    def compute(self):
        cached = self.dset.cached

        # check cache
        if cached:
            try:
                if self.cache_loc == self.dset.ctx.node.name:
                    # read locally
                    return self.dset._cache_provider.read(self.dset.id, self.idx)
                elif self.cache_loc:
                    peer = self.dset.ctx.node.peers[self.cache_loc]
                    return peer.run_task(lambda worker: self.dset._cache_provider.read(self.dset.id, self.idx)).result()
            except KeyError:
                pass
            except InvocationException:
                logger.exception('Unable to get cached partition %s.%s from %s',
                                 self.dset.id, self.idx, self.cache_loc)

        # compute if not cached
        data = self._compute()

        # cache if requested
        if cached:
            data = ensure_collection(data)
            self.dset._cache_provider.write(self.dset.id, self.idx, data)

        # return data
        return data


    @property
    def cache_loc(self):
        return self.dset._cache_locs.get(self.idx, None)


    def _compute(self):
        raise Exception('%s does not implement _compute' % type(self))


    def locality(self, workers):
        '''
        Determine locality of computing this partition at the given workers. 
        
        :return: a generator of worker, locality pairs.
        '''
        workers = set(workers)
        cache_loc = self.cache_loc
        if cache_loc:
            for worker in workers:
                if cache_loc == worker.name:
                    yield worker, PROCESS_LOCAL
                    workers.remove(worker)
                elif worker.islocal:
                    yield worker, NODE_LOCAL
                    workers.remove(worker)
        if workers:
            yield from self._locality(workers)


    def _locality(self, workers):
        '''
        Determine locality of computing this partition. Cache locality
        
        Typically source partition implementations which inherit from Partition
        indicate here whether computing on a worker shows locality. This allows
        dealing with caching and preference/requirements set at the data set
        separately from locality in the 'normal' case.
        '''
        src = self.src
        if src:
            if isinstance(src, Iterable):
                localities = defaultdict(int)
                forbidden = set()
                for src in src:
                    for worker, locality in src.locality(workers) or ():
                        if locality == FORBIDDEN:
                            forbidden.append(worker)
                        else:
                            localities[worker] += locality
                for worker in forbidden:
                    yield worker, FORBIDDEN
                    try:
                        localities[worker]
                    except KeyError:
                        pass
                src_count = len(self.src)
                for worker, locality in localities.items():
                    yield worker, locality / src_count
            else:
                return src.locality(workers)


    def __lt__(self, other):
        return self.id < other.id

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


    def __repr__(self):
        return '<%s %s.%s>' % ((self.__class__.__name__,) + self.id)



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


    def _compute(self):
        return self.iterable



class MaskedDataset(Dataset):
    def __init__(self, src, mask):
        super().__init__(src.ctx, src)
        self.mask = mask


    def parts(self):
        return self.mask(self.src.parts())



def _merge_multisource(others, cls):
    datasets = []
    def extend_or_append(other):
        if isinstance(other, cls):
            datasets.extend(other.src)
        else:
            datasets.append(other)
    for other in others:
        extend_or_append(other)

    for idx, dset in enumerate(datasets[:]):
        try:
            datasets.index(dset, idx + 1)
            datasets[idx] = dset._with()
        except ValueError:
            pass  # not found, not duplicated

    return datasets



class UnionDataset(Dataset):
    def __init__(self, src):
        assert len(src) > 1
        super().__init__(src[0].ctx, _merge_multisource(src, UnionDataset))


    def union(self, other, *others):
        return UnionDataset((self, other) + others)


    def parts(self):
        return list(chain.from_iterable(src.parts() for src in self.src))



class CartesianProductDataset(Dataset):
    def __init__(self, src):
        assert len(src) > 1
        super().__init__(src[0].ctx, _merge_multisource(src, CartesianProductDataset))


    def product(self, other, *others):
        return CartesianProductDataset((self, other) + others)


    def parts(self):
        part_pairs = product(*(ds.parts() for ds in self.src))
        return [CartesianProductPartition(self, idx, parts)
                for idx, parts in enumerate(part_pairs)]



class CartesianProductPartition(Partition):
    def _compute(self):
        yield from product(*tuple(src.compute() for src in self.src))



class TransformingDataset(Dataset):
    def __init__(self, src, *funcs):  # , descs):
        super().__init__(src.ctx, src)
        self.funcs = funcs
        self._pickle_funcs()


    def parts(self):
        return [
            TransformingPartition(self, i, part)
            for i, part in enumerate(self.src.parts())
        ]


    def map_partitions_with_part(self, func):
        if not self.cached:
            name, csite = get_callsite()
            # TODO name = self.name + ' -> ' + name
            dset = self._with(funcs=self.funcs + (func,),
                              name=name, get_callsite=csite)
            dset._pickle_funcs()
            return dset
        else:
            return TransformingDataset(self, func)


    def _pickle_funcs(self):
        try:
            self._funcs = pickle.dumps(self.funcs)
        except (pickle.PicklingError, AttributeError):
            self._funcs = cloudpickle.dumps(self.funcs)


    def __getstate__(self):
        state = dict(self.__dict__)
        del state['funcs']
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.funcs = pickle.loads(self._funcs)



class TransformingPartition(Partition):
    def _compute(self):
        data = self.src.compute()
        for func in self.dset.funcs:
            data = func(self.src, data if data is not None else ())
        return data



class ComputePartitionTask(RemoteTask):
    def __init__(self, part, group):
        name = part.dset.name
        super().__init__(part.dset.ctx, part.id, compute_part, (part,), {},
                         name=name, desc=part.dset.get_callsite, group=group)
        self.part = part
        self.locality = part.locality


    def execute(self, worker):
        dependencies_executed_on = defaultdict(list)
        for dep in self.dependencies:
            dependencies_executed_on[dep.executed_on[-1].name].append(dep.part.id)
        self.args += (dependencies_executed_on,)
        return super().execute(worker)


    def result(self):
        result = super().result()
        self._save_cacheloc(self.part)
        return result


    def _save_cacheloc(self, part):
        # memorize the cache location for the partition
        if part.dset.cached and not part.dset._cache_locs.get(part.idx):
            part.dset._cache_locs[part.idx] = self.executed_on[-1].name
        # traverse backup up the DAG
        # TODO don't we have to stop between shuffle write and read?
        if part.src:
            if isinstance(part.src, Iterable):
                for src in part.src:
                    self._save_cacheloc(src)
            else:
                self._save_cacheloc(part.src)


    def release(self):
        self.part = None



def compute_part(worker, part, dependencies_executed_on):
    try:
        # communicate out of band on which workers dependencies of this task were executed
        task_context()['dependencies_executed_on'] = dependencies_executed_on
        # generate data
        data = part.compute()
        # 'materialize' iterators and such for pickling
        if data is not None and not is_stable_iterable(data):
            return list(data)
        else:
            return data
    except TaskCancelled:
        return None
    except Exception:
        logger.info('error while computing part %s on worker %s',
                    part, worker, exc_info=True)
        raise
