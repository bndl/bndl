from bndl.compute.dataset.arrays import DistributedArray
from bndl.compute.dataset.tests import DatasetTest
import numpy as np


NUMERIC_TYPES = (
    np.bool8,
    np.int0,
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.uint0,
    np.uint8,
    np.uint16,
    np.uint32,
    np.uint64,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
    np.complex64,
    np.complex128,
    np.complex256,
)

OTHER_TYPES = (
    np.datetime64,
    np.str0,
)

ALL_TYPES = NUMERIC_TYPES + OTHER_TYPES


class ArrayTest(DatasetTest):
    def test_empty(self):
        self.assertEqual(self.ctx.array(range(0)).collect().shape, (0,))

    def test_collect(self):
        ten = self.ctx.array(range(10), pcount=4)
        self.assertEqual(ten.first(), 0)
        self.assertEqual(ten.count(), 10)
        self.assertEqual(len(ten.collect()), 10)
        self.assertEqual(list(ten.collect()), list(range(10)))
        self.assertEqual(ten.map_partitions(lambda p: [type(p)]).collect(), [np.ndarray] * 4)
        for part in ten.collect(parts=True):
            self.assertEqual(type(part), np.ndarray)


    def test_ctors(self):
        for size in (3, 9, 11, 51, 100, 102):
            zeros = self.ctx.zeros(size)
            self.assertEqual(zeros.count(), size)
            self.assertEqual(zeros.map_partitions(lambda p: [len(p)]).sum(), size)

        self.assertEqual(self.ctx.empty(100).count(), 100)
        self.assertEqual(type(self.ctx.ones(100, dtype=int).first()), np.int64)
        self.assertEqual(type(self.ctx.ones(100, dtype=int).sum()), np.int64)
        self.assertEqual(type(self.ctx.ones(100, dtype=int).mean()), np.float64)
        self.assertEqual(self.ctx.zeros(1000).mean(), 0)
        self.assertEqual(self.ctx.ones(1000).mean(), 1)


    def test_shape(self):
        for shape in ((100,), (6, 8), (3, 5, 8)):
            dset = self.ctx.array(np.zeros(shape))
            self.assertEqual(dset.shape, shape)


    def test_dtype(self):
        for nptype in NUMERIC_TYPES:
            dtype = np.dtype(nptype)
            a = np.zeros(10, dtype)
            dset = self.ctx.array(a)
            self.assertEqual(dset.dtype, dtype)
            self.assertEqual(type(dset.first()), nptype)
            self.assertEqual(dset.mean(), a.mean())


    def test_astype(self):
        dset = self.ctx.zeros(100, dtype=np.float64)
        dtype = np.dtype(np.float64)
        self.assertEqual(dset.dtype, dtype)
        self.assertEqual(dset.astype(np.float32).dtype, np.dtype(np.float32))


    def test_arange(self):
        ranges = [
            (self.ctx.arange(100), np.arange(100)),
            (self.ctx.arange(20, 100), np.arange(20, 100)),
            (self.ctx.arange(2.5, 100, 2.5), np.arange(2.5, 100, 2.5)),
            (self.ctx.arange(3, 17, 1.7), np.arange(3, 17, 1.7)),
        ]

        for dset, arange in ranges:
            dset_arange = dset.collect()
            self.assertEqual(dset_arange, arange)


    def test_sum(self):
        self.assertEqual(self.ctx.arange(100).sum(), np.arange(100).sum())
        base_shape = (4, 5, 5, 5)
        for shape_slice in range(2, 5):
            shape = base_shape[:shape_slice]
            size = np.prod(shape[:shape_slice])
            for axis in [None] + list(range(len(shape))):
                self.assertEqual(self.ctx.arange(size, pcount=4).reshape(shape).sum(axis=axis),
                                 np.arange(size).reshape(shape).sum(axis=axis))

    def test_mean(self):
        self.assertEqual(self.ctx.array(range(1, 100)).mean(), 50)
        self.assertEqual(self.ctx.arange(2.5, 100, 2.5).mean(), np.arange(2.5, 100, 2.5).mean())
        self.assertEqual(self.ctx.arange(1.3, 17, 1.7).mean(), np.arange(1.3, 17, 1.7).mean())

        for axis in (None, 0, 1, 2, 3):
            self.assertEqual(self.ctx.arange(500, pcount=4).reshape((4, 5, 5, 5)).mean(axis=axis),
                             np.arange(500).reshape((4, 5, 5, 5)).mean(axis=axis))


    def test_minmax(self):
        dset = self.ctx.arange(100, pcount=4)
        arr = np.arange(100)
        self.assertEqual(dset.min(), arr.min())
        self.assertEqual(dset.max(), arr.max())

        dset = dset.reshape((20, 5))
        arr = np.arange(100).reshape((20, 5))
        self.assertEqual(dset.min(), arr.min())
        self.assertEqual(dset.max(), arr.max())
        self.assertEqual(dset.min(axis=0), arr.min(axis=0))
        self.assertEqual(dset.max(axis=0), arr.max(axis=0))


    def test_reshape(self):
        shapes = [
            ((8, 2), 4),
            ((16, 6), 8),
            ((8, 6, 3), 8),
            ((4, 6, 3, 5), 4),

            ((5, 6), 5),
            ((5, 6, 3), 5),
            ((5, 6, 3, 5), 5),
        ]

        for shape, pcount in shapes:
            arange = np.arange(np.prod(shape))
            self.assertEqual(
                self.ctx.arange(np.prod(shape), pcount=pcount).reshape(shape),
                arange.reshape(shape)
            )
            self.assertEqual(
                self.ctx.array(arange.reshape(np.prod(shape))),
                arange
            )

        with self.assertRaises(ValueError):
            self.ctx.arange(100).reshape((10,))
        with self.assertRaises(ValueError):
            self.ctx.arange(100).reshape((10, 20))
        with self.assertRaises(ValueError):
            self.ctx.arange(100).reshape((10, 10))
        with self.assertRaises(ValueError):
            self.ctx.arange(110).reshape((11, 10))


    def assertEqual(self, first, second, rtol=1.e-5, atol=1.e-8, msg=None):
        if isinstance(first, DistributedArray):
            first = first.collect()
        if isinstance(second, DistributedArray):
            second = second.collect()
        if not (isinstance(first, np.ndarray) and isinstance(second, np.ndarray)):
            super().assertEqual(first, second, msg)
        elif not np.allclose(first, second, rtol=1.e-5, atol=1.e-8):
            self.failureException(
                msg or '%s != %s with %s relaltive tolerance and %s absolute tolerance' % (
                    first, second, rtol, atol
                )
            )
