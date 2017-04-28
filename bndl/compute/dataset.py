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

from collections import Counter, defaultdict, deque, Iterable, Sized, OrderedDict
from functools import partial, total_ordering, reduce
from itertools import islice, product, chain, starmap, groupby, count
from math import sqrt, log, ceil
from operator import add
import concurrent.futures
import gzip
import io
import json
import logging
import os
import pickle
import re
import shlex
import subprocess
import threading
import weakref

from cyhll import HyperLogLog
from cytoolz.functoolz import compose
from cytoolz.itertoolz import pluck, take

from bndl.compute import cache
from bndl.compute.job import RmiTask, Job, Task
from bndl.compute.scheduler import DependenciesFailed
from bndl.compute.scheduler import FailedDependency
from bndl.compute.stats import iterable_size, partial_mean, reduce_partial_means, \
                               Stats, MultiVariateStats, \
                               sample_with_replacement, sample_without_replacement
from bndl.compute.tasks import TaskCancelled
from bndl.compute.tasks import task_context, current_node
from bndl.net.connection import NotConnected
from bndl.net.rmi import InvocationException, root_exc
from bndl.util import strings
from bndl.util.callsite import get_callsite, callsite, set_callsite
from bndl.util.collection import is_stable_iterable, ensure_collection
from bndl.util.compat import lz4_compress
from bndl.util.exceptions import catch
from bndl.util.funcs import identity, getter, key_or_getter, partial_func
from bndl.util.hash import portable_hash
from bndl.util.strings import random_id
import cycloudpickle as cloudpickle
import cyheapq as heapq
import numpy as np


logger = logging.getLogger(__name__)


def traverse_dset_dag(*starts, depth_first=False, cond=None):
    if cond:
        def extend(q, d):
            for s in d.sources:
                if cond(d, s):
                    q.append(s)
    else:
        def extend(q, d):
            q.extend(d.sources)

    q = deque()
    q.extend(starts)

    pop = q.pop if depth_first else q.popleft

    while q:
        d = pop()
        yield d
        extend(q, d)



class Dataset(object):
    cleanup = None
    sync_required = False

    def __init__(self, ctx, src=None, dset_id=None):
        self.ctx = ctx
        self.src = src
        self.id = dset_id or random_id()
        self._cache_provider = None
        self._cache_locs = {}
        self._executors_required = None
        self.callsite = get_callsite(type(self))


    @property
    def sources(self):
        src = self.src
        if isinstance(src, Iterable):
            return tuple(src)
        elif src is not None:
            return (src,)
        else:
            return ()


    @property
    def pcount(self):
        pcount = getattr(self, '_pcount', None)
        if pcount is None:
            pcount = self._pcount = len(list(self.parts()))
        return pcount


    @pcount.setter
    def pcount(self, pcount):
        self._pcount = pcount


    def parts(self):
        return ()


    def map(self, func, *args, **kwargs):
        '''
        Transform elements in this dataset one by one.

        :param func: callable(element)
            applied to each element of the dataset

        Any extra \*args or \**kwargs are passed to func (args before element).
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions(partial(map, func))


    def starmap(self, func, *args, **kwargs):
        '''
        Variadic form of map.

        :param func: callable(element)
            applied to each element of the dataset

        Any extra \*args or \**kwargs are passed to func (args before element).
        '''
        func = partial_func(func, *args, **kwargs)
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


    def flatmap(self, func=None, *args, **kwargs):
        '''
        Transform the elements in this dataset into iterables and chain them
        within each of the partitions.

        :param func: callable(element)
            The transformation to apply. Defaults to none; i.e. consider the
            elements in this the iterables to chain.

        Any extra \*args or \**kwargs are passed to func (args before element).

        For example::

            >>> ''.join(ctx.collection(['abc']*10).flatmap().collect())
            'abcabcabcabcabcabcabcabcabcabc'

        or::

            >>> import string
            >>> ''.join(ctx.range(5).flatmap(lambda i: string.ascii_lowercase[i-1]*i).collect())
            'abbcccdddd'

        '''
        iterables = self.map(func, *args, **kwargs) if func else self
        return iterables.map_partitions(chain.from_iterable)


    def map_partitions(self, func, *args, **kwargs):
        '''
        Transform the partitions of this dataset.

        :param func: callable(iterator)
            The transformation to apply.

        Any extra \*args or \**kwargs are passed to func (args before element).
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions_with_part(lambda p, iterator: func(iterator))


    def map_partitions_with_index(self, func, *args, **kwargs):
        '''
        Transform the partitions - with their index - of this dataset.

        :param func: callable(\*args, index, iterator, \**kwargs)
            The transformation to apply on the partition index and the iterator
            over the partition's elements.

        Any extra \*args or \**kwargs are passed to func (args before index and iterator).
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions_with_part(lambda p, iterator: func(p.idx, iterator))


    def map_partitions_with_part(self, func, *args, **kwargs):
        '''
        Transform the partitions - with the source partition object as argument - of
        this dataset.

        :param func: callable(\*args, partition, iterator, \**kwargs)
            The transformation to apply on the partition object and the iterator
            over the partition's elements.

        Any extra \*args or \**kwargs are passed to func (args before partition and iterator).
        '''
        func = partial_func(func, *args, **kwargs)
        return TransformingDataset(self, func)


    def pipe(self, command, reader=io.IOBase.readlines, writer=io.IOBase.writelines, **opts):
        '''
        Transform partitions by sending partition data to an external program over stdout/stdin and
        read back its output.

        Args:
            command (str): Command to execute (is 'parsed' by ``shlex.split``).
            reader (callable -> iterable): Called with :attr:`subprocess.Popen.stdout` to read and
                iterate over the program's output.
            writer (callable): Called with :attr:`subprocess.Popen.stdin` and the partition
                iterable to write the partition's contents to the program.
                iterate over the program's output.
            **opts: Options to provide to :class:`subprocess.Popen`.
        '''
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
        '''
        Concattenate the elements in this dataset with sep

        Args:
            sep (str, bytes or bytarray): the value to use to concattenate.
        '''
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
                return (bytes(buffer),)
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


    def filter(self, func=None, *args, **kwargs):
        '''
        Filter out elements from this dataset

        Args:
            func (callable(element)): The test function to filter this dataset with. An element is
                retained in the dataset if the test is positive.

        Any extra \*args or \**kwargs are passed to func (args before element).
        '''
        if func:
            func = partial(func, *args, **kwargs)
        return self.map_partitions(partial(filter, func))


    def starfilter(self, func, *args, **kwargs):
        '''
        Variadic form of Dataset.filter.

        :param func: callable(\*element)
            The test function to filter this dataset with. An element is
            retained in the dataset if the test is positive. The element is
            provided as star args.

        Any extra \*args or \**kwargs are passed to func (args before element).
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions(lambda p: (e for e in p if func(*e)))


    def mask_partitions(self, mask):
        '''
        TODO
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
        key = key_or_getter(key)
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
        if callable(val):
            return self.map_partitions(lambda p: ((e, val(e)) for e in p))
        else:
            return self.map_partitions(lambda p: ((e, val) for e in p))


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


    def map_keys(self, func, *args, **kwargs):
        '''
        Transform the keys of this dataset.

        :param func: callable(key)
            Transformation to apply to the keys
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions(lambda p: ((func(k), v) for k, v in p))


    def map_values(self, func, *args, **kwargs):
        '''
        Transform the values of this dataset.

        :param func: callable(value)
            Transformation to apply to the values
        '''
        func = partial_func(func, *args, **kwargs)
        return self.map_partitions(lambda p: ((k, func(v)) for k, v in p))


    def pluck_values(self, ind, default=None):
        '''
        Pluck indices from each of the values in this dataset of (key, value) pairs.

        :param ind: obj or list
            The indices to pluck with.
        :param default: obj
            A default value.
        '''
        kwargs = {'default': default} if default is not None else {}
        return self.map_values(lambda value: pluck(ind, value, **kwargs))


    def flatmap_values(self, func=None, *args, **kwargs):
        '''
        :param func: callable(value) or None
            The callable which flattens the values of this dataset or None in
            order to use the values as iterables to flatten.
        '''
        func = partial_func(func, *args, **kwargs)
        return self.values().flatmap(func)


    def filter_bykey(self, func=None, *args, **kwargs):
        '''
        Filter the dataset by testing the keys.

        :param func: callable(key)
            The test to apply to the keys. When positive, the key, value tuple
            will be retained.
        '''
        if func:
            func = partial_func(func, *args, **kwargs)
            return self.map_partitions(lambda p: (kv for kv in p if func(kv[0])))
        else:
            return self.map_partitions(lambda p: (kv for kv in p if kv[0]))


    def filter_byvalue(self, func=None, *args, **kwargs):
        '''
        Filter the dataset by testing the values.

        :param func: callable(value)
            The test to apply to the values. When positive, the key, value tuple
            will be retained.
        '''
        if func:
            func = partial_func(func, *args, **kwargs)
            return self.map_partitions(lambda p: (kv for kv in p if func(kv[1])))
        else:
            return self.map_partitions(lambda p: (kv for kv in p if kv[1]))


    @callsite()
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


    @callsite()
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
        key = key_or_getter(key)
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
            with set_callsite(name='histogram.stats'):
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
            parts = self.map_partitions(lambda p: (local(p),)).icollect(ordered=False)
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
        subsequently shuffling the data across executors in depth rounds and for
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
        if depth is None:
            depth = 16
        if depth < 2:
            return self.aggregate(local, comb)

        pcount = len(self.parts())
        if scale is None:
            scale = max(int(ceil(pow(pcount, 1.0 / depth))), 2)
        pcount /= scale
        ipcount = round(pcount)

        if ipcount < 2:
            return self.aggregate(local, comb)

        if not comb:
            comb = local

        agg = self.map_partitions_with_index(lambda idx, p: [(idx % ipcount, local(p))])

        for _ in range(depth):
            agg = agg.aggregate_by_key(comb, pcount=ipcount, sort=False, **shuffle_opts)
            pcount /= scale
            ipcount = round(pcount)
            if pcount < scale:
                break
            agg = agg.map_keys(lambda idx, : idx % ipcount)

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

        Args:
            key (callable(element) or object): The (optional) key to apply in comparing element. If
            key is an object, it is used to pluck from the element with the given to get the
            comparison key.

        Example::

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

        Args:
            key (callable(element) or object): The (optional) key to apply in comparing element. If
            key is an object, it is used to pluck from the element with the given to get the
            comparison key.
        '''
        key = key_or_getter(key)
        return self.aggregate(partial(min, key=key) if key else min)


    def mean(self):
        '''
        Calculate the mean of this dataset.
        '''
        means = self.map_partitions(partial_mean).icollect()
        total, count = reduce_partial_means(means)
        return total / count


    def stats(self):
        '''
        Calculate count, mean, min, max, variance, stdev, skew and kurtosis of this dataset.
        '''
        return self.aggregate(Stats, partial(reduce, add))


    def mvstats(self, width=None):
        '''
        Calculate count and the multivariate mean, min, max, variance, stdev, skew and kurtosis of
        this dataset.

        Args:
            width (int): The width of the vectors in the dataset to calculate the statistics on. If
                no width is given, the width is determined through peeking at the first record.
        '''
        if width is None:
            width = len(self.first())
        return self.aggregate(partial(MultiVariateStats, width), partial(reduce, add))


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
        return (self.key_by(key)
                    .group_by_key(partitioner=partitioner, pcount=pcount, **shuffle_opts)
                    .map_values(list))


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
        return self._group_by_key(partitioner, pcount, **shuffle_opts).starmap(strip_key).map_values(list)


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
                yield key, list(product(*groups))

        return (self.cogroup(other, key=key, partitioner=partitioner, pcount=pcount, **shuffle_opts)
                    .flatmap(local_join))


    def product(self, other, *others, func=product):
        return CartesianProductDataset((self, other) + others, func=func)


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
        return self.aggregate(
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
            samples = self.sample(fraction).collect()
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
                        samples = samples1[:int(len(samples1) * fraction)] + samples2
                if len(samples) > point_count:
                    rng.shuffle(samples)
                    return samples[:point_count]
                else:
                    return samples
            with set_callsite(name='sort.sampler'):
                samples = self.map_partitions(sampler).collect()

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
        '''
        .. todo::

            Document shuffle
        '''
        key = key_or_getter(key)
        from .shuffle import ShuffleReadingDataset, ShuffleWritingDataset, ListBucket
        if bucket is None and sort == False:
            bucket = ListBucket
        shuffle = ShuffleWritingDataset(self, pcount, partitioner, bucket, key, comb, **opts)
        return ShuffleReadingDataset(shuffle, sort)


    def coalesce_parts(self, pcount):
        '''
        Coalesce partitions into pcount partitions.
        '''
        if pcount >= self.pcount:
            return self
        else:
            return CoalescedDataset(self, pcount)


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
        '''
        .. todo::

            Document sample
        '''
        if fraction == 0.0:
            return self.ctx.range(0)
        elif fraction == 1.0:
            return self

        assert 0 < fraction < 1

        if isinstance(seed, np.random.RandomState):
            rng = seed
        else:
            rng = np.random.RandomState(seed)

        sampling = sample_with_replacement if with_replacement else sample_without_replacement
        return self.map_partitions(partial(sampling, rng, fraction))


    # TODO implement stratified sampling

    def take_sample(self, num, with_replacement=False, seed=None, count=None):
        '''
        based on https://github.com/apache/spark/blob/master/python/pyspark/rdd.py#L425
        '''
        num = int(num)
        assert num >= 0
        if num == 0:
            return []

        if count is None:
            count = self.count()
        if count == 0:
            return []

        if isinstance(seed, np.random.RandomState):
            rng = seed
        else:
            rng = np.random.RandomState(seed)

        if (not with_replacement) and num >= count:
            samples = self.collect()
            rng.shuffle(samples)
            return samples

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
        '''
        Collect the dataset as list.

        Args:
            parts (bool): Collect the individual partitions (``True``) into a list with an element
                per partition or a 'flattened' list of the elements in the dataset (``False``).
            ordered (bool): Collect partitions (and thus their elements) in order or not.
        '''
        return list(self.icollect(parts, ordered))


    def collect_as_map(self, parts=False):
        '''
        Collect a dataset of key-value pairs as dict.
        '''
        dicts = self.map_partitions(dict)
        if parts:
            return dicts.collect(True, True)
        else:
            combined = {}
            for d in dicts.icollect(True, False):
                combined.update(d)
            return combined


    def collect_as_set(self):
        '''
        Collect the elements of the dataset into a set.
        '''
        s = set()
        for part in self.map_partitions(set).icollect(True, False):
            s.update(part)
        return s


    def _convert_for_save(self, mode, compression, ext):
        if mode not in ('t', 'b'):
            raise ValueError('mode should be t(ext) or b(inary)')
        data = self
        # compress if necessary
        if compression is not None:
            if mode == 't':
                data = data.map(lambda e: e.encode())
            if compression == 'gzip':
                ext += '.gz'
                compress = gzip.compress
            elif compression == 'lz4':
                ext += '.lz4'
                compress = lz4_compress
            elif compression is not None:
                raise ValueError('Only gzip and lz4 compression is supported')
            # compress concatenation of partition, not just each element
            mode = 'b'
            data = data.concat(b'').map(compress)
        # add an index to the partitions (for in the filename)
        data = data.map_partitions_with_index(lambda idx, part: (idx, ensure_collection(part)))
        # return updated mode, ext and converted data
        return mode, ext, data


    def _save_partition(self, directory, ext, mode, idx, part):
        directory = os.path.expanduser(directory)
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, '%s%s' % (idx, ext)), 'w' + mode) as f:
            f.writelines(part)


    def collect_as_pickles(self, directory=None, compression=None):
        '''
        Save each partition as a pickle file into directory at the driver.
        '''
        self.glom().map(pickle.dumps).collect_as_files(directory, '.p', 'b', compression)


    def save_as_pickles(self, directory=None, compression=None):
        '''
        Collect each partition as a pickle file into directory at the executors.
        '''
        self.glom().map(pickle.dumps).save_as_files(directory, '.p', 'b', compression)


    def collect_as_json(self, directory=None, compression=None):
        '''
        Collect each partition as a line separated json file into directory at the driver.
        '''
        self.map(json.dumps).concat(os.linesep).collect_as_files(directory, '.json', 't', compression)


    def save_as_json(self, directory=None, compression=None):
        '''
        Save each partition as a line separated json file into directory at the executors.
        '''
        self.map(json.dumps).concat(os.linesep).save_as_files(directory, '.json', 't', compression)


    def collect_as_files(self, directory=None, ext='', mode='b', compression=None):
        '''
        Collect each partition in this data set into a file into directory at the driver.

        :param directory: str
            The directory to save this data set to.
        :param ext:
            The extenion of the files.
        :param compress: None or 'gzip' or 'lz4'
            Whether to compress.
        '''
        mode, ext, data = self._convert_for_save(mode, compression, ext)
        for idx, part in data.icollect(ordered=False, parts=True):
            self._save_partition(directory or os.getcwd(), ext, mode, idx, part)


    def save_as_files(self, directory=None, ext='', mode='b', compression=None):
        '''
        Save each partition in this data set into a file into directory at the executors.

        :param directory: str
            The directory to save this data set to.
        :param ext:
            The extenion of the files.
        :param compress: None or 'gzip' or 'lz4'
            Whether to compress.
        '''
        mode, ext, data = self._convert_for_save(mode, compression, ext)
        data.glom().starmap(self._save_partition, (directory or os.getcwd()), ext, mode).execute()



    def execute(self):
        def consume_iterable(i):
            if not is_stable_iterable(i):
                sum(1 for _ in i)
        for _ in self.map_partitions(consume_iterable)._execute(ordered=False):
            pass


    def icollect(self, parts=False, ordered=True):
        '''
        An iterable version of :meth:`collect` which can be cheaper memory-wise and faster in terms
        of latency (especially of ordered=False).
        '''
        result = self._execute(ordered)
        result = filter(lambda p: p is not None, result)  # filter out empty parts
        if not parts:
            result = chain.from_iterable(result)  # chain the parts into a big iterable
        return result


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
        assert self.ctx.running, 'context of dataset is not running'
        remaining = num
        sliced = self.map_partitions(partial(take, remaining))._itake_parts()
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


    def _itake_parts(self):
        mask = slice(0, 1)
        pcount = len(self.parts())
        while mask.start < pcount:
            masked = self.mask_partitions(lambda part: mask.start <= part.idx < mask.stop)
            done = self.ctx.execute(masked._schedule())
            try:
                for task in done:
                    yield task.result()
                mask = slice(mask.stop, mask.stop * 2 + 1)
            finally:
                done.close()


    def require_executors(self, executors_required):
        '''
        Create a clone of this dataset which is only computable on the executors
        which are returned by the executors_required argument.

        :param executors_required: function(iterable[PeerNode]): -> iterable[PeerNode]
            A function which is given an iterable of executors (PeerNodes) which is to
            return only those which are allowed to compute this dataset.
        '''
        return self._with(_executors_required=executors_required)


    def require_local_executors(self):
        '''
        Require that the dataset is computed on the same node as the driver.
        '''
        return self.require_executors(lambda executors: [executor.islocal() for executor in executors])


    def allow_all_executors(self):
        '''
        Create a clone of this dataset which resets the executor filter set by
        require_executors if any.
        '''
        if self._executors_required is not None:
            return self._with(_executors_required=None)
        else:
            return self


    def _execute(self, ordered=True):
        assert self.ctx.running, 'context of dataset is not running'
        job = self._schedule()
        done = self.ctx.execute(job, order_results=ordered)
        for task in done:
            yield task.result()


    def _generate_tasks(self, required, tasks, groups):
        group = next(groups)
        dset_tasks = [
            tasks.get(part.id) or ComputePartitionTask(part, group=group)
            for part in self.parts()
        ]

        for d in traverse_dset_dag(*self.sources, cond=lambda f, t: not f.sync_required):
            if d.sync_required:
                dependencies = d._generate_tasks(
                    required,
                    tasks,
                    groups
                )

                barrier_id = d.id, len(dependencies)
                barrier = tasks.get(barrier_id) or BarrierTask(d.ctx, barrier_id, group='hidden')
                tasks[barrier_id] = barrier

                barrier.dependents.update(dset_tasks)
                barrier.dependencies.update(dependencies)

                for task in dset_tasks:
                    task.dependencies.add(barrier)

                mark_done = d not in required
                for dependency in dependencies:
                    dependency.dependents.add(barrier)
                    if mark_done:
                        dependency.mark_done()

        for task in dset_tasks:
            tasks[task.id] = task

        return dset_tasks


    def _tasks(self):
        required = {self}
        for dset in traverse_dset_dag(self, cond=lambda f, t: not t._cache_available):
            required.add(dset)

        tasks = OrderedDict()
        self._generate_tasks(required, tasks, count())

        groups = max(task.group
                     for task in tasks.values()
                     if task.group != 'hidden')

        taskslist = []
        for priority, task in enumerate(tasks.values()):
            task.priority = priority
            taskslist.append(task)
            if task.group != 'hidden':
                task.group = groups - task.group + 1

        return taskslist


    def _schedule(self):
        tasks = self._tasks()

        name, desc = get_callsite(__file__)
        name = re.sub('[_.]', ' ', name or '')
        job = Job(self.ctx, tasks, name, desc)

        cleanups = []
        stack = [self]
        while stack:
            dset = stack.pop()
            stack.extend(dset.sources)
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
        '''
        Cache the dataset in the executors. Each partition is cached in the memory / on the disk of
        the executor which computed it.

        Args:
            location (str): 'memory' or 'disk'.
            serialization (str): The serialization format must be one of 'json', 'marshal',
                'pickle', 'msgpack', 'text', 'binary' or None to cache the data unserialized.
            compression (str): 'gzip' or None
            provider (:class:`CacheProvider <bndl.compute.cache.CacheProvider>`): Ignore location,
                serialization and compression and use this custom ``CacheProvider``.
        '''
        assert self.ctx.node.node_type == 'driver'
        if location or provider:
            if location == 'disk' and not serialization:
                serialization = 'pickle'
            if provider:
                self._cache_provider = provider
            elif self._cache_provider:
                self._cache_provider.modify(location, serialization, compression)
            else:
                self._cache_provider = cache.CacheProvider(self.ctx, location, serialization, compression)
        else:
            self.uncache()
        return self


    @property
    def cached(self):
        return bool(self._cache_provider)


    @property
    def _cache_available(self):
        return bool(self.cached and self._cache_locs)


    def uncache(self, block=False, timeout=None):
        if not self.cached:
            return

        logger.info('Uncaching %r', self)

        # issue uncache tasks
        def clear(provider=self._cache_provider, dset_id=self.id):
            provider.clear(dset_id)

        tasks = [executor.service('tasks').execute for executor in self.ctx.executors]
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
                    logger.warning('Error while uncaching %s', self, exc_info=True)

        # clear cache locations
        self._cache_locs = {}
        self._cache_provider = None

        return self


    def __del__(self):
        if getattr(self, '_cache_provider', None):
            try:
                node = None
                with catch(RuntimeError):
                    node = self.ctx.node
                if node and node.node_type == 'driver':
                    self.uncache()
            except:
                logger.exception('Unable to clear cache')


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


    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop('callsite', None)
        return state


    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)


    def __str__(self):
        callsite = getattr(self, 'callsite')
        if callsite and callsite[0]:
            return '%s (%s)' % (callsite[0], self.__class__.__name__)
        else:
            return self.__class__.__name__


FORBIDDEN = -1
NON_LOCAL = 0
LOCAL = 1
NODE_LOCAL = 3
PROCESS_LOCAL = 5


@total_ordering
class Partition(object):
    def __init__(self, dset, idx, src=None):
        self.dset = weakref.proxy(dset)
        self.idx = idx
        self.src = src


    @property
    def sources(self):
        dset = self.dset
        if isinstance(dset.src, Dataset) and dset.src.sync_required:
            return tuple(dset.src.parts())
        else:
            src = self.src
            if isinstance(src, Iterable):
                return tuple(src)
            elif src is not None:
                return (src,)
            else:
                return ()


    def compute(self):
        cached = self.dset.cached
        cache_loc = self.cache_loc()

        # check cache
        if cached and cache_loc:
            try:
                if cache_loc == self.dset.ctx.node.name:
                    logger.debug('Using local cache for %r', self)
                    return self.dset._cache_provider.read(self.dset.id, self.idx)
                else:
                    logger.debug('Using remote cache for %r on %r', self, cache_loc)
                    peer = self.dset.ctx.node.peers[cache_loc]
                    cp = self.dset._cache_provider
                    part_id = self.id
                    return peer.service('tasks').execute(lambda: cp.read(*part_id)).result()
            except KeyError:
                pass
            except NotConnected:
                logger.info('Unable to get cached partition %s.%s from %s (not connected)',
                            self.dset.id, self.idx, cache_loc)
            except InvocationException as exc:
                exc = root_exc(exc)
                if isinstance(exc, KeyError):
                    logger.info('Unable to get cached partition %s.%s from %s (not found)',
                                self.dset.id, self.idx, cache_loc)
                else:
                    logger.warning('Unable to get cached partition %s.%s from %s',
                                   self.dset.id, self.idx, cache_loc, exc_info=True)
            except Exception:
                logger.warning('Unable to get cached partition %s.%s from %s',
                               self.dset.id, self.idx, cache_loc, exc_info=True)

        # compute if not cached
        logger.debug('Computing %r', self)
        data = self._compute()

        # cache if requested
        if cached:
            logger.debug('Caching %r', self)
            data = ensure_collection(data)
            self.dset._cache_provider.write(self.dset.id, self.idx, data)
        
        return data


    def cache_loc(self):
        return self.dset._cache_locs.get(self.idx, None)


    def _compute(self):
        raise Exception('%s does not implement _compute' % type(self))


    def locality(self, executors):
        '''
        Determine locality of computing this partition at the given executors.

        :return: a generator of executor, locality pairs.
        '''
        if self.dset._executors_required is not None:
            allowed = set(self.dset._executors_required(executors))
            forbidden = set(executors) - allowed
            executors = allowed
            for executor in forbidden:
                yield executor, FORBIDDEN

        cache_loc = self.cache_loc()
        if cache_loc:
            executors_filtered = set(executors)
            for executor in executors:
                if cache_loc == executor.name:
                    yield executor, PROCESS_LOCAL
                    executors_filtered.remove(executor)
                elif executor.islocal:
                    yield executor, NODE_LOCAL
                    executors_filtered.remove(executor)
            executors = executors_filtered

        if executors:
            yield from self._locality(executors)


    def _locality(self, executors):
        '''
        Determine locality of computing this partition. Cache locality is dealt
        with when this method is invoked by Partition.locality(executors).

        Typically source partition implementations which inherit from Partition
        indicate here whether computing on a executor shows locality. This allows
        dealing with caching and preference/requirements set at the data set
        separately from locality in the 'normal' case.

        :param executors: An iterable of executors.
        :returns: An iterable of (executor, locality:int) tuples indicating the
        executor locality; locality 0 can be omitted as this is the default
        locality.
        '''
        if self.src:
            if isinstance(self.src, Iterable):
                localities = defaultdict(int)
                forbidden = set()
                for src in self.src:
                    if src.dset.sync_required:
                        continue

                    for executor, locality in src.locality(executors) or ():
                        if locality == FORBIDDEN:
                            forbidden.append(executor)
                        else:
                            localities[executor] += locality
                for executor in forbidden:
                    yield executor, FORBIDDEN
                    try:
                        del localities[executor]
                    except KeyError:
                        pass
                src_count = len(self.src)
                for executor, locality in localities.items():
                    yield executor, locality / src_count
            else:
                src = self.src
                if not src.dset.sync_required:
                    yield from src.locality(executors)


    def save_cache_location(self, executor):
        try:
            dset = self.dset
            dset.id
        except ReferenceError:
            return

        # memorize the cache location for the partition
        if dset.cached:
            loc = dset._cache_locs.get(self.idx)
            peer = dset.ctx.node.peers.get(loc)
            if not loc or (peer or not peer.is_connected):
                dset._cache_locs[self.idx] = executor
        # traverse backup up the task (not the entire DAG)
        if self.src:
            if isinstance(self.src, Iterable):
                for src in self.src:
                    if not src.dset.sync_required:
                        src.save_cache_location(executor)
            else:
                src = self.src
                if not src.dset.sync_required:
                    src.save_cache_location(executor)


    @property
    def id(self):
        return (self.dset.id, self.idx)


    def __lt__(self, other):
        return self.id < other.id

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


    def __repr__(self):
        return '<%s %s.%s>' % ((self.__class__.__name__,) + self.id)



class MaskedDataset(Dataset):
    def __init__(self, src, mask):
        super().__init__(src.ctx, src)
        self.mask = mask


    def parts(self):
        return filter(self.mask, self.src.parts())



def _merge_multisource(others, cls):
    datasets = []
    def extend_or_append(other):
        if isinstance(other, cls):
            datasets.extend(other.src)
        else:
            datasets.append(other)
    for other in others:
        extend_or_append(other)

    return datasets



class _MultiSourceDataset(Dataset):
    def __init__(self, src):
        assert len(src) > 1
        super().__init__(src[0].ctx, _merge_multisource(src, type(self)))


class UnionDataset(_MultiSourceDataset):
    def union(self, other, *others):
        return UnionDataset((self, other) + others)


    def parts(self):
        parts = chain.from_iterable(src.parts() for src in self.src)
        return list(UnionPartition(self, idx, part)
                    for idx, part in enumerate(parts))



class UnionPartition(Partition):
    def _compute(self):
        return self.src.compute()



class CartesianProductDataset(_MultiSourceDataset):
    def __init__(self, src, func=product):
        super().__init__(src)
        self.func = func


    def product(self, other, *others):
        return CartesianProductDataset((self, other) + others)


    def parts(self):
        part_pairs = product(*(ds.parts() for ds in self.src))
        return [CartesianProductPartition(self, idx, parts)
                for idx, parts in enumerate(part_pairs)]


    def __getstate__(self):
        state = super().__getstate__()
        state['func'] = cloudpickle.dumps(self.func)
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.func = pickle.loads(self.func)



class CartesianProductPartition(Partition):
    def _compute(self):
        yield from self.dset.func(*tuple(src.compute() for src in self.src))



class CoalescedDataset(Dataset):
    def __init__(self, src, pcount):
        assert pcount >= 1
        super().__init__(src.ctx, src)
        self.pcount = pcount


    def parts(self):
        src_parts = list(self.src.parts())

        step = len(src_parts) / self.pcount
        assert step > 0.5

        batches = []
        offset = 0
        for _ in range(self.pcount - 1):
            batches.append(src_parts[int(offset):int(offset + step)])
            offset += step

        remainder = src_parts[int(offset):]
        if remainder:
            batches.append(remainder)

        return [CoalescedPartition(self, idx, src=b)
                for idx, b in enumerate(batches)]



class CoalescedPartition(Partition):
    def _compute(self):
        for src in self.src:
            yield from src.compute()



class TransformingDataset(Dataset):
    def __init__(self, src, *funcs):
        super().__init__(src.ctx, src)
        self.funcs = funcs
        self._pickle_funcs()


    def parts(self):
        return [
            TransformingPartition(self, i, part)
            for i, part in enumerate(self.src.parts())
        ]


    def map_partitions_with_part(self, func):
        if self.cached:
            return TransformingDataset(self, func)
        else:
            callsite = get_callsite(__file__)
            # TODO name = self.callsite[0] + ' -> ' + name
            funcs = self.funcs + (func,)
            dset = self._with(funcs=funcs, callsite=callsite)
            dset._pickle_funcs()
            return dset


    def _pickle_funcs(self):
        self._funcs = cloudpickle.dumps(self.funcs)


    def __getstate__(self):
        state = super().__getstate__()
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



class BarrierTask(Task):
    '''
    An 'artificial' task which disconnects
    :class:`ComputePartitionTasks <bndl.compute.dataset.ComputePartitionTask>` in order to reduce
    computational complexity in :mod:`bndl.compute.scheduler`. As this scheduler tracks individual
    dependencies the scheduling overhead goes through the roof when even a moderate amount of tasks
    which depend on another set of tasks (which is the case for a shuffle). E.g. consider

    .. code:: python

        ctx.range(1000, pcount=1000).map(lambda i: (i//10, i)).aggregate_by_key(sum).count()

    This would result in 1000 mapper and 1000 reducer tasks and thus in 1.000.000 dependencies to
    be tracked. After introducing the BarrierTask, there are 1000 + 1000 + 1 tasks and 1000 + 1000
    dependencies to track.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dependency_locations = None


    def execute(self, scheduler, executor):
        # administer where dependencies were executed
        by_location = self.dependency_locations = {}
        for dep in self.dependencies:
            exc_on = dep.last_executed_on()
            if not exc_on:
                assert dep.done
            result_on = dep.last_result_on()
            locs = by_location.get(result_on)
            if locs is None:
                locs = by_location[result_on] = {}
            locs = locs.get(exc_on)
            if locs is None:
                locs = by_location[result_on][exc_on] = []
            locs.append(dep.part.id)

        # 'execute' the barrier
        self.set_executing(executor)
        future = self.future = concurrent.futures.Future()
        future.set_result(None)
        self.signal_stop()
        return future


class ComputePartitionTask(RmiTask):
    '''
    A RMI task to compute a partition (and it's 'narrow' sources). It adds some dependency location
    tracking and communications as well as memorizing where a partition was computed and cached.
    '''

    def __init__(self, part, **kwargs):
        name = re.sub('[_.]', ' ', part.dset.callsite[0] or '')
        super().__init__(part.dset.ctx, part.id, _compute_part, [part, None], {},
                         name=name, desc=part.dset.callsite, **kwargs)
        self.part = part
        self.locality = part.locality


    def execute(self, scheduler, executor):
        if self.dependencies:
            # created mapping of executor -> list[part_id] for dependency locations
            dependency_locations = {}
            for barrier in self.dependencies:
                dependency_locations.update(barrier.dependency_locations)
            # set locations as second arguments to _compute_part
            self.args[1] = dependency_locations
        return super().execute(scheduler, executor)


    def signal_stop(self):
        if self.dependents and self.succeeded and self.result():
            self.result_on[-1] = self.result()[0]
            self.future.set_result(None)
        if self.dependencies and self.failed:
            exc = root_exc(self.exception())
            if isinstance(exc, DependenciesFailed) or isinstance(exc, FailedDependency):
                for barrier in self.dependencies:
                    logger.debug('Marking barrier %r before %r as failed', barrier, self)
                    barrier.mark_failed(FailedDependency())
        super().signal_stop()


    def release(self):
        if self.succeeded:
            self.part.save_cache_location(self.last_executed_on())
        self.part = None
        super().release()



def _compute_part(part, dependency_locations):
    '''
    Compute a partition; this method calls compute for a partition (which calls comppute on its
    source partition(s), reads data from some external source, from cache, from other executors,
    etc.). This method is the method to execute by :class:`bndl.compute.dataset.ComputePartitionTask`
    (which delegates the RMI part to :class:`bndl.compute.jobs.RmiTask`.

    Args:
        part: The partition to compute.
        dependency_locations (mapping[str,object]): A mapping of executor name to a sequence of
            partition ids executed by this executor.
    '''
    try:
        # communicate out of band on which executors dependencies of this task were executed
        task_context()['dependency_locations'] = dependency_locations
        # generate data
        data = part.compute()
        # 'materialize' iterators and such for pickling
        if data is not None and not is_stable_iterable(data):
            return list(data)
        else:
            return data
    except TaskCancelled:
        return None
    except DependenciesFailed:
        raise
    except Exception:
        logger.info('Error while computing part %s on executor %s',
                    part, current_node(), exc_info=True)
        raise
