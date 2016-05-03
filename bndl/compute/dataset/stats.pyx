from _collections_abc import Sized

cpdef iterable_size(i):
    cdef long s
    if isinstance(i, Sized):
        return (len(i),)
    else:
        s = 0
        for _ in i:
            s += 1
        return (s,)


# TODO doesn't scale to large numbers
cpdef local_mean(p):
    cdef double total
    cdef long count
    i = iter(p)
    if isinstance(p, Sized):
        return [(sum(i), len(p))]
    else:
        total = next(i)
        count = 1
        for e in i:
            total += e
            count += 1
        return [(total, count)]


cpdef reduce_mean(p):
    cdef double total
    cdef long count
    i = iter(p)
    total, count = next(i)
    for a, b in i:
        total += a
        count += b
    return (total, count)

# import math
# 
# 
# class Metric(object):
#     '''
#         Calculates a running mean, variance, standard deviation, skew and
#         kurtosis for a metric. The metric can be updated by calling the metric.
#         
#         Based on http://www.johndcook.com/blog/skewness_kurtosis/
#     '''
#     
#     def __init__(self):
#         self.total = 0
#         self.n = 0
#         self.m1 = 0.0
#         self.m2 = 0.0
#         self.m3 = 0.0
#         self.m4 = 0.0
#         self._lock = asyncio.Lock()
#         
#     def push(self, amount):
#         self.total = amount
#         n1 = self.n
#         self.n += 1
#         
#         Δ = amount - self.m1
#         Δn = Δ / self.n
#         Δn2 = Δn * Δn
#         term1 = Δ * Δn * n1
#         
#         self.m1 += Δn
#         self.m4 += term1 * Δn2 * (self.n * self.n - 3 * self.n + 3) + 6 * Δn2 * self.m2 - 4 * Δn * self.m3
#         self.m3 += term1 * Δn * (self.n - 2) - 3 * Δn * self.m2
#         self.m2 += term1
#     
#     def clear(self):
#         pass
#         
#     @property
#     def count(self):
#         return self.n
#     
#     @property
#     def mean(self):
#         return self.m1
#         
#     @property
#     def var(self):
#         return self.m2 / (self.n - 1)
#         
#     @property
#     def std(self):
#         return math.sqrt(self.var)
#         
#     @property
#     def skew(self):
#         return math.sqrt(self.n) * self.m3 / pow(self.m2, 1.5);
#         
#     @property
#     def kurt(self):
#         return self.n * self.m4 / (self.m2 * self.m2) - 3.0;
# 
#     def __add__(self, b):
#         a = self
#         
#         c = Metric()
#         c.n = self.n + b.n
#         
#         Δ = b.M1 - a.M1;
#         Δ2 = Δ * Δ;
#         Δ3 = Δ * Δ2;
#         Δ4 = Δ2 * Δ2;
#         
#         c.m1 = (a.n * a.m1 + b.n * b.m1) / c.n
#         
#         c.m2 = a.m2 + b.m2 + Δ2 * a.n * b.n / c.n
#         
#         c.m3 = a.m3 + b.m3 + Δ3 * a.n * b.n * (a.n - b.n) / (c.n * c.n)
#         c.m3 += 3.0 * Δ * (a.n * b.m2 - b.n * a.m2) / c.n
#         
#         c.m4 = a.m4 + b.m4
#         c.m4 += Δ4 * a.n * b.n * (a.n * a.n - a.n * b.n + b.n * b.n) / (c.n * c.n * c.n)
#         c.m4 += 6.0 * Δ2 * (a.n * a.n * b.m2 + b.n * b.n * a.m2) / (c.n * c.n)
#         c.m4 += 4.0 * Δ * (a.n * b.m3 - b.n * a.m3) / c.n
#         
#         return c
# 
# class CategoriesMetric(object):
#     def __init__(self):
#         self.categories = {}
#         
#     def push(self, category, amount=1):
#         try:
#             metric = self.categories[category]
#         except KeyError:
#             self.categories[category] = metric = Metric()
#         metric.push(amount)            


