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

from statistics import mean

from bndl.compute.dense import DistributedArray
from bndl.compute.tests import DatasetTest
import numpy as np
import inspect


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
        self.assertEqual(self.ctx.dense.array(range(0)).collect().shape, (0,))

    def test_collect(self):
        ten = self.ctx.dense.array(range(10), pcount=4)
        self.assertEqual(ten.first(), 0)
        self.assertEqual(ten.take(4), np.arange(4))
        self.assertEqual(list(ten.itake(4)), [0, 1, 2, 3])
        self.assertEqual(ten.count(), 10)
        self.assertEqual(len(ten.collect()), 10)
        self.assertEqual(list(ten.collect()), list(range(10)))
        self.assertEqual(ten.map_partitions(lambda p: [type(p)]).collect(), [np.ndarray] * 4)
        for part in ten.collect(parts=True):
            self.assertEqual(type(part), np.ndarray)


    def test_ctors(self):
        for size in (3, 9, 11, 51, 100, 102):
            zeros = self.ctx.dense.zeros(size)
            self.assertEqual(zeros.count(), size)
            self.assertEqual(zeros.map_partitions(lambda p: [len(p)]).sum(), size)

        self.assertEqual(self.ctx.dense.empty(100).count(), 100)
        self.assertEqual(type(self.ctx.dense.ones(100, dtype=int).first()), np.int64)
        self.assertEqual(type(self.ctx.dense.ones(100, dtype=int).sum()), np.int64)
        self.assertEqual(type(self.ctx.dense.ones(100, dtype=int).mean()), np.float64)
        self.assertEqual(self.ctx.dense.zeros(1000).mean(), 0)
        self.assertEqual(self.ctx.dense.ones(1000).mean(), 1)


    def test_shape(self):
        for shape in ((100,), (6, 8), (3, 5, 8)):
            dset = self.ctx.dense.array(np.zeros(shape))
            self.assertEqual(dset.shape, shape)


    def test_dtype(self):
        for nptype in NUMERIC_TYPES:
            dtype = np.dtype(nptype)
            a = np.zeros(10, dtype)
            dset = self.ctx.dense.array(a)
            self.assertEqual(dset.dtype, dtype)
            self.assertEqual(type(dset.first()), nptype)
            self.assertEqual(dset.mean(), a.mean())


    def test_astype(self):
        dset = self.ctx.dense.zeros(100, dtype=np.float64)
        dtype = np.dtype(np.float64)
        self.assertEqual(dset.dtype, dtype)
        self.assertEqual(dset.astype(np.float32).dtype, np.dtype(np.float32))


    def test_arange(self):
        ranges = [
            (self.ctx.dense.range(100), np.arange(100)),
            (self.ctx.dense.range(20, 100), np.arange(20, 100)),
            (self.ctx.dense.range(2.5, 100, 2.5), np.arange(2.5, 100, 2.5)),
            (self.ctx.dense.range(3, 17, 1.7), np.arange(3, 17, 1.7)),
        ]

        for dset, arange in ranges:
            self.assertEqual(dset.collect(), arange)


    def test_sum(self):
        self.assertEqual(self.ctx.dense.range(100).sum(), np.arange(100).sum())
        base_shape = (4, 5, 5, 5)
        for shape_slice in range(2, 5):
            shape = base_shape[:shape_slice]
            size = np.prod(shape[:shape_slice])
            for axis in [None] + list(range(len(shape))):
                self.assertEqual(self.ctx.dense.range(size, pcount=4).reshape(shape).sum(axis=axis),
                                 np.arange(size).reshape(shape).sum(axis=axis))

    def test_mean(self):
        self.assertEqual(self.ctx.dense.array(range(1, 100)).mean(), 50)
        self.assertEqual(self.ctx.dense.range(2.5, 100, 2.5).mean(), np.arange(2.5, 100, 2.5).mean())
        self.assertEqual(self.ctx.dense.range(1.3, 17, 1.7).mean(), np.arange(1.3, 17, 1.7).mean())

        for axis in (None, 0, 1, 2, 3):
            self.assertEqual(self.ctx.dense.range(500, pcount=4).reshape((4, 5, 5, 5)).mean(axis=axis),
                             np.arange(500).reshape((4, 5, 5, 5)).mean(axis=axis))


    def test_minmax(self):
        dset = self.ctx.dense.range(100, pcount=4)
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
                self.ctx.dense.range(np.prod(shape), pcount=pcount).reshape(shape),
                arange.reshape(shape)
            )
            self.assertEqual(
                self.ctx.dense.array(arange.reshape(np.prod(shape))),
                arange
            )

        with self.assertRaises(ValueError):
            self.ctx.dense.range(100).reshape((10,))
        with self.assertRaises(ValueError):
            self.ctx.dense.range(100).reshape((10, 20))
        with self.assertRaises(ValueError):
            self.ctx.dense.range(100).reshape((10, 10))
        with self.assertRaises(ValueError):
            self.ctx.dense.range(110).reshape((11, 10))


    def test_cache(self):
        arr = self.ctx.dense.range(100)
        m1 = arr.mean()
        self.assertEqual(m1, mean(range(100)))

        arr.cache()
        m2 = arr.mean()
        self.assertEqual(m1, m2)

        arr.cache(False)
        m3 = arr.mean()
        self.assertEqual(m2, m3)


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


    def test_random(self):
        vals = {
            'binomial': {'p': .5},
            'dirichlet': {'alpha': (2, 2)},
            'geometric': {'p': .5},
            'logseries': {'p': .5},
            'multinomial': {'pvals': list(range(3))},
            'multivariate_normal': {'mean': np.random.rand(3), 'cov': np.random.rand(3, 3)},
            'negative_binomial': {'p': .5},
            'randint': {'low': 0},
            'triangular': {'left': 0, 'mode': 2, 'right': 4},
        }
        
        for member in dir(self.ctx.dense.random):
            if member.startswith('_'):
                continue
            member = getattr(self.ctx.dense.random, member)
            sig = inspect.signature(member)
            args = []
            for param in sig.parameters:
                if param not in ('size', 'dtype', 'pcount'):
                    args.append(vals.get(member.__name__, {}).get(param, 3))
            print(member.__name__, sig, args)
            member(*args, size=(10, 10), pcount=3).collect()
