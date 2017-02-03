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

from collections import Sized, Sequence
from functools import partial
import math

import numpy as np

import array
from cpython cimport array
cimport cython

cpdef iterable_size(i):
    cdef long s = 0
    if isinstance(i, Sized):
        return len(i)
    else:
        for _ in i:
            s += 1
        return s


cpdef sample_with_replacement(rng, fraction, partition):
    if isinstance(partition, Sequence):
        return [
            e for e, count in zip(partition, rng.poisson(fraction, len(partition)))
              for _ in range(count)
        ]
    else:
        return [
            e for e in partition
              for _ in range(rng.poisson(fraction))
        ]


cpdef sample_without_replacement(rng, fraction, partition):
    if isinstance(partition, Sized):
        return [
            e for e, sample in zip(partition, rng.random_sample(len(partition)))
              if sample < fraction
        ]
    else:
        return [
            e for e in partition
              if rng.poisson() < fraction
        ]


def _reconstruct_stats(ctor, n, min, max, m1, m2, m3, m4):
    stats = ctor()
    stats._n = n
    stats._min = min
    stats._max = max
    stats._m1 = m1
    stats._m2 = m2
    stats._m3 = m3
    stats._m4 = m4
    return stats


cdef class _StatProps:
    @property
    def count(self):
        return self._n

    @property
    def mean(self):
        return self._m1

    @property
    def min(self):  # @ReservedAssignment
        return self._min

    @property
    def max(self):  # @ReservedAssignment
        return self._max

    @property
    def stdev(self):
        return np.sqrt(self.variance)

    @property
    def sample_stdev(self):
        return np.sqrt(self.sample_variance)

    @property
    def variance(self):
        if self._n == 0:
            return self.nan
        return self._m2 / self._n

    @property
    def sample_variance(self):
        if self._n <= 1:
            return self.nan
        return self._m2 / (self._n - 1)

    @property
    def skew(self):
        if np.all(self._m2 == 0):
            return self.nan
        return math.sqrt(self._n) * self._m3 / pow(self._m2, 1.5)

    @property
    def kurtosis(self):
        if np.all(self._m2 == 0):
            return self.nan
        return self._n * self._m4 / (self._m2 * self._m2) - 3.0



cdef class Stats(_StatProps):
    '''
    Calculates a running mean, variance, standard deviation, skew and
    kurtosis for a metric. The metric can be updated by calling the metric.

    Based on http://www.johndcook.com/blog/skewness_kurtosis/
    and https://github.com/apache/spark/blob/master/python/pyspark/statcounter.py
    '''

    cdef public long _n
    cdef public double _m1
    cdef public double _m2
    cdef public double _m3
    cdef public double _m4
    cdef public double _min
    cdef public double _max

    def __init__(self, values=()):
        self._n = 0
        self._m1 = 0.0
        self._m2 = 0.0
        self._m3 = 0.0
        self._m4 = 0.0
        self._min = float('inf')
        self._max = float('-inf')

        for v in values:
            self.push(v)


    cpdef push(self, double value):
        if value < self._min:
            self._min = value
        if value > self._max:
            self._max = value

        cdef double n1 = self._n
        self._n += 1

        cdef double d = value - self._m1
        cdef double dn = d / self._n
        cdef double dn2 = dn * dn
        cdef double term1 = d * dn * n1

        self._m1 += dn
        self._m4 += term1 * dn2 * (self._n * self._n - 3 * self._n + 3) + 6 * dn2 * self._m2 - 4 * dn * self._m3
        self._m3 += term1 * dn * (self._n - 2) - 3 * dn * self._m2
        self._m2 += term1


    def __add__(self, b):
        return self.add(b)


    cpdef add(self, Stats b):
        cdef Stats a = self
        cdef Stats c = Stats()

        c._n = a._n + b._n
        if c._n == 0:
            return c

        c._min = min(a._min, b._min)
        c._max = max(a._max, b._max)

        cdef double d = b._m1 - a._m1;
        cdef double d2 = d * d;
        cdef double d3 = d * d2;
        cdef double d4 = d2 * d2;

        c._m1 = (a._n * a._m1 + b._n * b._m1) / c._n

        c._m2 = a._m2 + b._m2 + d2 * a._n * b._n / c._n

        c._m3 = a._m3 + b._m3 + d3 * a._n * b._n * (a._n - b._n) / (c._n * c._n)
        c._m3 += 3.0 * d * (a._n * b._m2 - b._n * a._m2) / c._n

        c._m4 = a._m4 + b._m4
        c._m4 += d4 * a._n * b._n * (a._n * a._n - a._n * b._n + b._n * b._n) / (c._n * c._n * c._n)
        c._m4 += 6.0 * d2 * (a._n * a._n * b._m2 + b._n * b._n * a._m2) / (c._n * c._n)
        c._m4 += 4.0 * d * (a._n * b._m3 - b._n * a._m3) / c._n

        return c


    @property
    def nan(self):
        return np.nan


    def __reduce__(self):
        return _reconstruct_stats, (Stats, self._n, self.min, self.max, self._m1, self._m2, self._m3, self._m4)


    def __repr__(self):
        return '<Stats count=%s, mean=%s, min=%s, max=%s, var=%s, stdev=%s, skew=%s, kurt=%s>' % (
            self.count, self.mean, self.min, self.max, self.variance, self.stdev, self.skew, self.kurtosis
        )



cdef class MultiVariateStats(_StatProps):
    '''
    Adaptation of :class:`Stats` to support multivariate statistics (summary statistics per column).
    '''

    cdef public int width
    cdef public long _n
    cdef public _m1
    cdef public _m2
    cdef public _m3
    cdef public _m4
    cdef public _min
    cdef public _max


    def __init__(self, width, vectors=None):
        cdef object vector

        self.width = width
        self._n = 0
        self._m1 = np.zeros(width)
        self._m2 = np.zeros(width)
        self._m3 = np.zeros(width)
        self._m4 = np.zeros(width)
        self._min = np.repeat(float('inf'), width)
        self._max = np.repeat(float('-inf'), width)

        if vectors is not None:
            if isinstance(vectors, np.ndarray):
                if vectors.dtype == np.dtype('f16'):
                    vectors = vectors.astype('f8')
                elif vectors.dtype.kind != 'f':
                    itemsize = max(min(vectors.dtype.itemsize, 8), 4)
                    vectors = vectors.astype('f' + str(itemsize))

                if vectors.dtype == np.dtype('f4'):
                    for vector in vectors:
                        self._push[cython.float](vector)
                else:
                    assert vectors.dtype == np.dtype('f8')
                    for vector in vectors:
                        self._push[cython.double](vector)
            else:
                for vector in vectors:
                    self.push(vector)


    cpdef push(self, vector):
        try:
            self._push[cython.double](vector)
        except TypeError:
            self._push[cython.double](array.array('d', vector))


    cdef _push(self, cython.floating[:] vector):
        cdef double[:] m1 = self._m1
        cdef double[:] m2 = self._m2
        cdef double[:] m3 = self._m3
        cdef double[:] m4 = self._m4
        cdef double[:] min = self._min
        cdef double[:] max = self._max

        cdef long n1 = self._n
        cdef long n = n1 + 1
        self._n = n

        cdef int i
        cdef double value, d, dn, dn2, term1

        for i in range(self.width):
            value = vector[i]
            if value < min[i]:
                min[i] = value
            if value > max[i]:
                max[i] = value

            d = value - m1[i]
            dn = d / self._n
            dn2 = dn * dn
            term1 = d * dn * n1

            m1[i] += dn
            m4[i] += term1 * dn2 * (n * n - 3 * n + 3) + 6 * dn2 * m2[i] - 4 * dn * m3[i]
            m3[i] += term1 * dn * (n - 2) - 3 * dn * m2[i]
            m2[i] += term1


    def __add__(self, b):
        return self.add(b)


    cpdef add(self, MultiVariateStats b):
        assert self.width == b.width
        cdef MultiVariateStats a = self
        cdef MultiVariateStats c = MultiVariateStats(self.width)

        c._n = a._n + b._n
        if c._n == 0:
            return c

        c._min = np.minimum(a._min, b._min)
        c._max = np.maximum(a._max, b._max)

        d = b._m1 - a._m1;
        d2 = d * d;
        d3 = d * d2;
        d4 = d2 * d2;

        c._m1 = (a._n * a._m1 + b._n * b._m1) / c._n

        c._m2 = a._m2 + b._m2 + d2 * a._n * b._n / c._n

        c._m3 = a._m3 + b._m3 + d3 * a._n * b._n * (a._n - b._n) / (c._n * c._n)
        c._m3 += 3.0 * d * (a._n * b._m2 - b._n * a._m2) / c._n

        c._m4 = a._m4 + b._m4
        c._m4 += d4 * a._n * b._n * (a._n * a._n - a._n * b._n + b._n * b._n) / (c._n * c._n * c._n)
        c._m4 += 6.0 * d2 * (a._n * a._n * b._m2 + b._n * b._n * a._m2) / (c._n * c._n)
        c._m4 += 4.0 * d * (a._n * b._m3 - b._n * a._m3) / c._n

        return c


    @property
    def nan(self):
        return np.repeat(np.nan, self.width)


    def __reduce__(self):
        return _reconstruct_stats, (partial(MultiVariateStats, self.width), self._n, self.min, self.max, self._m1, self._m2, self._m3, self._m4)


    def __repr__(self):
        return '<Stats count=%s, mean=%s, min=%s, max=%s, var=%s, stdev=%s, skew=%s, kurt=%s>' % (
            self.count, self.mean, self.min, self.max, self.variance, self.stdev, self.skew, self.kurtosis
        )
