from collections import Sized, Sequence
import math


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



def _reconstruct_stats(n, min, max, m1, m2, m3, m4):
    stats = Stats()
    stats._n = n
    stats._min = min
    stats._max = max
    stats._m1 = m1
    stats._m2 = m2
    stats._m3 = m3
    stats._m4 = m4
    return stats


cdef class Stats:
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
    def variance(self):
        if self._n == 0:
            return float('nan')
        return self._m2 / self._n

    @property
    def sample_variance(self):
        if self._n <= 1:
            return float('nan')
        return self._m2 / (self._n - 1)

    @property
    def stdev(self):
        return math.sqrt(self.variance)

    @property
    def sample_stdev(self):
        return math.sqrt(self.sample_variance)

    @property
    def skew(self):
        if not self._m2:
            return float('nan')
        return math.sqrt(self._n) * self._m3 / pow(self._m2, 1.5)

    @property
    def kurtosis(self):
        if not self._m2:
            return float('nan')
        return self._n * self._m4 / (self._m2 * self._m2) - 3.0

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

    def __reduce__(self):
        return _reconstruct_stats, (self._n, self.min, self.max, self._m1, self._m2, self._m3, self._m4)

    def __repr__(self):
        return '<Stats count=%s, mean=%s, min=%s, max=%s, var=%s, stdev=%s, skew=%s, kurt=%s>' % (
            self.count, self.mean, self.min, self.max, self.variance, self.stdev, self.skew, self.kurtosis
        )

# TODO
# class KeyedStats(object):
#     def __init__(self):
#         self.categories = {}
#
#     def push(self, category, value=1):
#         try:
#             metric = self.categories[category]
#         except KeyError:
#             self.categories[category] = metric = Metric()
#         metric.push(value)
