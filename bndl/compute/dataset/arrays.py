import abc
import collections
import numpy as np

from bndl.compute.dataset.base import Dataset, Partition
from bndl.compute.dataset.collections import DistributedCollection


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
        parts = iter(sliced.icollect(eager=False, parts=True))
        collected = next(parts)
        for part in parts:
            collected = np.concatenate([collected, part])
        parts.close()
        return collected[:num]


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
    def empty(ctx, shape, dtype=float, pcount=None):
        return DistributedArray.fill(ctx, np.empty, shape, dtype, pcount)

    @staticmethod
    def zeros(ctx, shape, dtype=float, pcount=None):
        return DistributedArray.fill(ctx, np.zeros, shape, dtype, pcount)

    @staticmethod
    def ones(ctx, shape, dtype=float, pcount=None):
        return DistributedArray.fill(ctx, np.ones, shape, dtype, pcount)

    @staticmethod
    def fill(ctx, fill, shape, dtype=float, pcount=None):
        def ctor(part, shape, dtype):
            return fill(shape, dtype=dtype)
        return CtorDistributedArray(ctx, ctor, shape, dtype, pcount)

    @staticmethod
    def arange(ctx, start, stop=None, step=1, dtype=None, pcount=None):
        if not stop:
            stop, start = start, 0

        shape = int(
            np.ceil((stop - start) / step)
            if start < stop and step > 0
            else 0
        )

        def ctor(part, shape, dtype):
            offset = part.dset._slice_start(part.idx)
            part_start = start + step * offset
            # part_stop is actually correct end of the sub range (last value in the sub range)
            # but rounding causes it to vary whether np.arange will include part_stop or not
            # therefore half the step is added to ensure that it _is_ added
            part_stop = part_start + (shape[0] - 1) * step + step / 2
            return np.arange(part_start, part_stop, step, dtype)
        return CtorDistributedArray(ctx, ctor, shape, dtype, pcount)


class SourceDistributedArray(DistributedArray, DistributedCollection):
    def __init__(self, ctx, arr, pcount=None, psize=None):
        if not isinstance(arr, np.ndarray):
            arr = np.array(arr)
        super().__init__(ctx, arr, pcount=None, psize=None)
        self.arr = arr
        self.dtype = arr.dtype
        self.shape = arr.shape

    def parts(self):
        parts = super().parts()
        for part in parts:
            part.shape = (len(part.iterable),) + self.shape[1:]
        return parts


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

    def _materialize(self, ctx):
        return self.ctor(self, self.shape, self.dset.dtype)



class TransformedDistributedArray(DistributedArray):
    def __init__(self, src, derivation):
        super().__init__(src.ctx, src, derivation.id)
        self.dtype = src.dtype
        self.shape = src.shape
        self.derivation = derivation

    def parts(self):
        return self.derivation.parts()
