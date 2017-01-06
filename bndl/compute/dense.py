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

from functools import partial
import abc
import collections

from bndl.compute.collections import DistributedCollection
from bndl.compute.dataset import Dataset, Partition
from bndl.util.funcs import as_method
from bndl.util.objects import ExtensionGroup
import numpy as np


class DistributedArray(Dataset, metaclass=abc.ABCMeta):
    dtype = None
    shape = None
    pcount = None

    def collect(self, parts=False):
        if parts:
            return super().collect(parts=True)
        else:
            parts = super().icollect(parts=True)
            parts = [p for p in parts if p.shape]
            if parts:
                return np.concatenate(parts)
            else:
                return np.empty(0, dtype=self.dtype)

    def take(self, num):
        sliced = self.map_partitions(lambda p: p[0:num])
        parts = sliced._itake_parts()
        try:
            collected = [next(parts)]
            remaining = num - len(collected[0])
            while remaining > 0:
                try:
                    part = next(parts)
                    collected.append(part)
                    remaining -= len(part)
                except StopIteration:
                    break
            collected = np.concatenate(collected)
            return collected[:num]
        finally:
            parts.close()


    def sum(self, axis=None, dtype=None):
        res = np.array(self.map_partitions(lambda a: [a.sum(axis, dtype=dtype)]).collect())
        if axis is None or axis < len(self.shape) - 1:
            return res.sum(axis=axis)
        else:
            return res

    def mean(self, axis=None):
        if axis in (None, 0):
            if axis is None:
                def local_mean(arr):
                    return [(arr.mean(), np.prod(arr.shape))]
            else:
                def local_mean(arr):
                    return [(arr.mean(0), np.prod(arr.shape[1:]))]
            means_weights = np.array(self.map_partitions(local_mean).collect()).T
            return np.average(means_weights[0], weights=means_weights[1])
        else:
            return np.array(self.map_partitions(lambda a: a.mean(axis=axis)).collect())


    def min(self, axis=None):
        if axis not in (None, 0):
            raise ValueError("Can't determine max over axis other than the distribution axis")
        return np.array(self.map_partitions(lambda p: [p.min(axis=axis)]).collect()).min(axis=axis)


    def max(self, axis=None):
        if axis not in (None, 0):
            raise ValueError("Can't determine max over axis other than the distribution axis")
            # TODO possibly implement this through a transpose (shuffle) ...
        return np.array(self.map_partitions(lambda p: [p.max(axis=axis)]).collect()).max(axis=axis)


    def mvstats(self, width=None):
        '''
        Calculate multivariate statistics. See also :meth:`bndl.compute.dataset.Dataset.mvstats`.

        With a two dimensional distributed array (matrix), the width is a given, so there's no need
        to provide it. The width argument is only kept for compatibility.
        '''
        assert len(self.shape) == 2
        assert width is None or width == self.shape[1]
        return Dataset.mvstats(self, self.shape[1])


    def astype(self, dtype):
        return self._transformed(
            self.map_partitions(lambda a: a.astype(dtype)),
            dtype=dtype
        )


    def reshape(self, shape):
        if np.prod(shape) != np.prod(self.shape):
            raise ValueError('total size of new distributed array must be unchanged')
        elif (self.shape[0] % self.pcount) != 0:
            raise ValueError('can only reshape if 0-axis of source dataset is'
                             'divided in equally along the %s partitions' % self.pcount)
        elif (shape[0] % self.pcount) != 0:
            raise ValueError('can only reshape if 0-axis is divided in equal'
                             'chunks across the %s partitions' % self.pcount)
        ratio_axis0 = shape[0] / self.shape[0]
        return self._transformed(self.map_partitions_with_part(
            lambda p, a: a.reshape((int(ratio_axis0 * p.shape[0]),) + shape[1:])
        ), shape=shape)


#     def __add__(self, value):
#         return self._ufunc(np.add, value)
#
#     def __sub__(self, value):
#         return self._ufunc(np.add, value)
#
#     def _ufunc(self, f, value):
#         if isinstance(value, DistributedArray):
#             # TODO if shape matches, maybe not so difficult, need to shuflle probably
#             # if shapes don't match ... see dim_diff related block below
#             raise ValueError("Can't apply ufuncs on distributed arrays just yet")
#         elif isinstance(value, numbers.Number):
#             value = np.array(value)
#
#         dim_diff = len(value.shape) < len(self.shape)
#         if dim_diff < 0:
#             val_shape = tuple([()] * -dim_diff) + value.shape
#         elif dim_diff > 0:
#             # TODO maybe collect and then broadcast?
#             raise ValueError("Can't broadcast a distributed array just yet")
#         else:
#             val_shape = value.shape



#         broadcast = (
#             isinstance(value, numbers.Number) or
#             (isinstance(value, nd.ndarray) and value.shape != self.shape)
#         )
#
#         if isinstance(value, numbers.Number):
#             return self._transformed(self.map_partitions(lambda a: a + value))
#         elif isinstance(value, DistributedArray):
#             pass
#         elif isinstance(value, np.ndarray):
#             value = self.ctx.array(value, pcount=self.pcount, psize=self.psize)
#         else:
#             raise ValueError("can't add %s to %s" % (value, self))
#
#
#
#     def __getitem__(self, key):
#         if isinstance(key, int):
#             key = slice(key, key + 1)
#
#         if isinstance(key, slice):
#             key = (key,)
#         elif isinstance(key, tuple):
#             if not all(isinstance(e, (int, slice)) for e in key):
#                 raise TypeError(key)
#             key = [
#                 slice(e, e + 1)
#                 if isinstance(e, int)
#                 else e
#                 for e in key
#             ]
#         else:
#             raise TypeError(key)
#
#         if len(self.shape) - len(key) < 0:
#             raise ValueError("to many indices for array")
#
#         def posidx(length, idx):
#             return idx if idx >= 0 else length + idx
#
#         def inslice(v, s, l):
#             return (v >= s.start) or (v < s.stop)
#
#         def mask(parts):
#             selected = []
#             start = 0
#             for part in parts:
#                 stop = start + part.shape[0]
#                 if (inslice(start, key[0], self.shape[0]) or inslice(stop, key[0], self.shape[0])):
#                     part.offset = start
#                     selected.append(part)
#                 if key[0].stop and start > posidx(self.shape[0], key[0].stop):
#                     break
#                 start += stop
#             return selected
#
#         def map_subarray(part, array):
#             # TODO negative ranges!
#             slices = (slice(key[0].start - part.offset, key[0].stop - part.offset, key[0].step),) + tuple(
#                 key[1:]
#             )
#
#             return array[slices]
#
#         transformed = self.mask_partitions(mask).map_partitions_with_part(map_subarray)
#
#         shape = ...  # self.shape
#
#         return self._transformed(transformed, shape=shape)


    def _transformed(self, transformed, shape=None, dtype=None):
        arr = TransformedDistributedArray(self, transformed)
        arr.dtype = dtype or self.dtype
        arr.shape = shape or self.shape
        return arr

    @staticmethod
    def fill(ctx, fill, shape, dtype=float, pcount=None):
        def ctor(part, shape):
            return fill(shape)
        return CtorDistributedArray(ctx, ctor, shape, dtype, pcount)


class SourceDistributedArray(DistributedArray, DistributedCollection):
    '''
    Distribute a numpy array across pcount partitions or in psize partitions. Note that the array
    is partitioned along axis 0.

    Args:
        arr (np.ndarray): The array to partition and distribute.
        pcount (int or None): The number of partitions.
        psize (int or None): The maximum size of the partitions.
    '''
    def __init__(self, ctx, arr, pcount=None, psize=None):
        if not isinstance(arr, np.ndarray):
            arr = np.array(arr)
        super().__init__(ctx, arr, pcount, psize)
        self.arr = arr
        self.dtype = arr.dtype
        self.shape = arr.shape

    def parts(self):
        parts = super().parts()
        for part in parts:
            part.shape = (part.length,) + self.shape[1:]
        return parts

    def __getstate__(self):
        state = super().__getstate__()
        del state['arr']
        return state


class CtorDistributedArray(DistributedArray):
    def __init__(self, ctx, ctor, shape, dtype, pcount=None):
        super().__init__(ctx)
        self.dtype = dtype
        self.shape = shape if isinstance(shape, collections.Sized) else (shape,)
        self.ctor = ctor
        self.pcount = pcount or ctx.default_pcount

    def parts(self):
        shapes = (
            (idx, self._slice_shape(idx))
            for idx in range(self.pcount)
        )

        return [
            CtorPartition(self, idx, self.ctor, shape)
            for idx, shape in shapes
            if shape[0]
        ]

    def _slice_shape(self, idx):
        start, end = self._slice_start(idx), self._slice_start(idx + 1)
        return (end - start,) + self.shape[1:]

    def _slice_start(self, idx):
        return idx * self.shape[0] // self.pcount



class CtorPartition(Partition):
    def __init__(self, dset, idx, ctor, shape):
        super().__init__(dset, idx)
        self.ctor = ctor
        self.shape = shape

    def _compute(self):
        return self.ctor(self, self.shape)



class TransformedDistributedArray(DistributedArray):
    def __init__(self, src, derivation):
        super().__init__(src.ctx, src, derivation.id)
        self.dtype = src.dtype
        self.shape = src.shape
        self.derivation = derivation

    def parts(self):
        return self.derivation.parts()


class random(ExtensionGroup):
    # ==============================================================================
    # Utility functions
    # ==============================================================================
    def random(self, size, pcount=None):
        '''Uniformly distributed values of a given shape.'''
        return DistributedArray.fill(self, np.random.random, size, pcount=pcount)

    def random_integers(self, low, high, size, pcount=None):
        '''Uniformly distributed integers in a given range.'''
        return DistributedArray.fill(self, partial(np.random.random_integers, low, high), size, pcount=pcount)

    def random_sample(self, size, pcount=None):
        '''Uniformly distributed floats in a given range.'''
        return DistributedArray.fill(self, np.random.random_sample, size, pcount=pcount)


    # ==============================================================================
    # Compatibility functions
    # ==============================================================================

    def rand(self, size, pcount=None):
        '''Uniformly distributed values.'''
        return DistributedArray.fill(self, lambda size: np.random.rand(*size), size, pcount=pcount)

    def randn(self, size, pcount=None):
        '''Normally distributed values.'''
        return DistributedArray.fill(self, lambda size: np.random.randn(*size), size, pcount=pcount)

    def ranf(self, size, pcount=None):
        '''Uniformly distributed floating point numbers.'''
        return DistributedArray.fill(self, np.random.ranf, size, pcount=pcount)

    def randint(self, low, high, size, dtype=np.int, pcount=None):
        '''Uniformly distributed integers in a given range.'''
        return DistributedArray.fill(self, partial(np.random.randint, low, high), size, dtype, pcount)


    # ==============================================================================
    # Univariate distributions
    # ==============================================================================

    def beta(self, a, b, size, pcount=None):
        '''Beta distribution over ``[0, 1]``.'''
        return DistributedArray.fill(self, partial(np.random.beta, a, b), size, pcount=pcount)

    def binomial(self, n, p, size, pcount=None):
        '''Binomial distribution.'''
        return DistributedArray.fill(self, partial(np.random.binomial, n, p), size, pcount=pcount)

    def chisquare(self, df, size, pcount=None):
        ''':math:`\\chi^2` distribution.'''
        return DistributedArray.fill(self, partial(np.random.chisquare, df), size, pcount=pcount)

    def exponential(self, scale, size, pcount=None):
        '''Exponential distribution.'''
        return DistributedArray.fill(self, partial(np.random.exponential, scale), size, pcount=pcount)

    def f(self, dfnum, dfden, size, pcount=None):
        '''F (Fisher-Snedecor) distribution.'''
        return DistributedArray.fill(self, partial(np.random.f, dfnum, dfden), size, pcount=pcount)

    def gamma(self, shape, scale, size, pcount=None):
        '''Gamma distribution.'''
        return DistributedArray.fill(self, partial(np.random.gamma, shape, scale), size, pcount=pcount)

    def geometric(self, p, size, pcount=None):
        '''Geometric distribution.'''
        return DistributedArray.fill(self, partial(np.random.geometric, p), size, pcount=pcount)

    def gumbel(self, loc, scale, size, pcount=None):
        '''Gumbel distribution.'''
        return DistributedArray.fill(self, partial(np.random.gumbel, loc, scale), size, pcount=pcount)

    def hypergeometric(self, ngood, nbad, nsample, size, pcount=None):
        '''Hypergeometric distribution.'''
        return DistributedArray.fill(self, partial(np.random.hypergeometric, ngood, nbad, nsample), size, pcount=pcount)

    def laplace(self, loc, scale, size, pcount=None):
        '''Laplace distribution.'''
        return DistributedArray.fill(self, partial(np.random.laplace, loc, scale), size, pcount=pcount)

    def logistic(self, loc, scale, size, pcount=None):
        '''Logistic distribution.'''
        return DistributedArray.fill(self, partial(np.random.logistic, loc, scale), size, pcount=pcount)

    def lognormal(self, mean, sigma, size, pcount=None):
        '''Log-normal distribution.'''
        return DistributedArray.fill(self, partial(np.random.lognormal, mean, sigma), size, pcount=pcount)

    def logseries(self, p, size, pcount=None):
        '''Logarithmic series distribution.'''
        return DistributedArray.fill(self, partial(np.random.logseries, p), size, pcount=pcount)

    def negative_binomial(self, n, p, size, pcount=None):
        '''Negative binomial distribution.'''
        return DistributedArray.fill(self, partial(np.random.negative_binomial, n, p), size, pcount=pcount)

    def noncentral_chisquare(self, df, nonc, size, pcount=None):
        '''Non-central chi-square distribution.'''
        return DistributedArray.fill(self, partial(np.random.noncentral_chisquare, df, nonc), size, pcount=pcount)

    def noncentral_f(self, dfnum, dfden, nonc, size, pcount=None):
        '''Non-central F distribution.'''
        return DistributedArray.fill(self, partial(np.random.noncentral_f, dfnum, dfden, nonc), size, pcount=pcount)

    def normal(self, loc, scale, size, pcount=None):
        '''Normal / Gaussian distribution.'''
        return DistributedArray.fill(self, partial(np.random.normal, loc, scale), size, pcount=pcount)

    def pareto(self, a, size, pcount=None):
        '''Pareto distribution.'''
        return DistributedArray.fill(self, partial(np.random.pareto, a), size, pcount=pcount)

    def poisson(self, lam, size, pcount=None):
        '''Poisson distribution.'''
        return DistributedArray.fill(self, partial(np.random.poisson, lam), size, pcount=pcount)

    def power(self, a, size, pcount=None):
        '''Power distribution.'''
        return DistributedArray.fill(self, partial(np.random.power, a), size, pcount=pcount)

    def rayleigh(self, scale, size, pcount=None):
        '''Rayleigh distribution.'''
        return DistributedArray.fill(self, partial(np.random.rayleigh, scale), size, pcount=pcount)

    def triangular(self, left, mode, right, size, pcount=None):
        '''Triangular distribution.'''
        return DistributedArray.fill(self, partial(np.random.triangular, left, mode, right), size, pcount=pcount)

    def uniform(self, low, high, size, pcount=None):
        '''Uniform distribution.'''
        return DistributedArray.fill(self, partial(np.random.uniform, low, high), size, pcount=pcount)

    def vonmises(self, mu, kappa, size, pcount=None):
        '''Von Mises circular distribution.'''
        return DistributedArray.fill(self, partial(np.random.vonmises, mu, kappa), size, pcount=pcount)

    def wald(self, mean, scale, size, pcount=None):
        '''Wald (inverse Gaussian) distribution.'''
        return DistributedArray.fill(self, partial(np.random.wald, mean, scale), size, pcount=pcount)

    def weibull(self, a, size, pcount=None):
        '''Weibull distribution.'''
        return DistributedArray.fill(self, partial(np.random.weibull, a), size, pcount=pcount)

    def zipf(self, a, size, pcount=None):
        '''Zipf's distribution over ranked data.'''
        return DistributedArray.fill(self, partial(np.random.zipf, a), size, pcount=pcount)


    # ==============================================================================
    # Multivariate distributions
    # ==============================================================================

    def dirichlet(self, alpha, size, pcount=None):
        '''Multivariate generalization of Beta distribution.'''
        return DistributedArray.fill(self, partial(np.random.dirichlet, alpha), size, pcount=pcount)

    def multinomial(self, n, pvals, size, pcount=None):
        '''Multivariate generalization of the binomial distribution.'''
        return DistributedArray.fill(self, partial(np.random.multinomial, n, pvals), size, pcount=pcount)

    def multivariate_normal(self, mean, cov, size, pcount=None):
        '''Multivariate generalization of the normal distribution.'''
        return DistributedArray.fill(self, partial(np.random.multivariate_normal, mean, cov), size, pcount=pcount)


    # ==============================================================================
    # Standard distributions
    # ==============================================================================

    def standard_cauchy(self, size, pcount=None):
        '''Standard Cauchy-Lorentz distribution.'''
        return DistributedArray.fill(self, np.random.standard_cauchy, size, pcount=pcount)

    def standard_exponential(self, size, pcount=None):
        '''Standard exponential distribution.'''
        return DistributedArray.fill(self, np.random.standard_cauchy, size, pcount=pcount)

    def standard_gamma(self, shape, size, pcount=None):
        '''Standard Gamma distribution.'''
        return DistributedArray.fill(self, partial(np.random.standard_gamma, shape), size, pcount=pcount)

    def standard_normal(self, size, pcount=None):
        '''Standard normal distribution.'''
        return DistributedArray.fill(self, np.random.standard_normal, size, pcount=pcount)

    def standard_t(self, df, size, pcount=None):
        '''Standard Student's t-distribution.'''
        return DistributedArray.fill(self, partial(np.random.standard_t, df), size, pcount=pcount)



class arrays(ExtensionGroup):
    '''
    Create numpy based distributed, partitioned, dense arrays.
    '''
    random = property(random)
    array = as_method(SourceDistributedArray)

    def range(self, start, stop=None, step=1, dtype=None, pcount=None):
        '''
        A distributed and partitioned `np.arange` like data set.

        Args:
            start (int): The start or stop value (if no stop value is given).
            stop (int): The stop value or None if start is the stop value.
            step (int): The step between each value in the range.
            dtype: The type of the elements in the array.
            pcount (int): The number of partitions to partition the range into.
        '''
        if not stop:
            stop, start = start, 0

        shape = int(
            np.ceil((stop - start) / step)
            if start < stop and step > 0
            else 0
        )

        def ctor(part, shape):
            offset = part.dset._slice_start(part.idx)
            part_start = start + step * offset
            # part_stop is actually correct end of the sub range (last value in the sub range)
            # but rounding causes it to vary whether np.arange will include part_stop or not
            # therefore half the step is added to ensure that it _is_ added
            part_stop = part_start + (shape[0] - 1) * step + step / 2
            return np.arange(part_start, part_stop, step, dtype)
        return CtorDistributedArray(self, ctor, shape, dtype, pcount)

    def empty(self, shape, dtype=float, pcount=None):
        '''A distributed and partitioned version of `np.empty`.'''
        return DistributedArray.fill(self, partial(np.empty, dtype=dtype), shape, dtype, pcount)

    def zeros(self, shape, dtype=float, pcount=None):
        '''A distributed and partitioned version of `np.zeros`.'''
        return DistributedArray.fill(self, partial(np.zeros, dtype=dtype), shape, dtype, pcount)

    def ones(self, shape, dtype=float, pcount=None):
        '''A distributed and partitioned version of `np.ones`.'''
        return DistributedArray.fill(self, partial(np.ones, dtype=dtype), shape, dtype, pcount)
