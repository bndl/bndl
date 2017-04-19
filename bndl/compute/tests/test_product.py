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

from functools import reduce
from itertools import product
import string

from bndl.compute.tests import ComputeTest


class CartesianProductTest(ComputeTest):
    def test_product(self):
        a = string.ascii_letters
        b = string.ascii_lowercase
        c = string.digits
        abc = list(product(a, b, map(int, c)))

        dsets = [
            self.ctx.collection(a, pcount=3).map_partitions(lambda p: iter(p)),
            self.ctx.collection(b, pcount=4).map_partitions(lambda p: sorted(p, reverse=True)),
            self.ctx.collection(c, pcount=5).map(int),
        ]

        dset = reduce(lambda a, b: a.product(b), dsets)
        self.assertEqual(sorted(dset.collect()), sorted(abc))


    def test_self_product(self):
        dset = self.ctx.collection(string.ascii_letters)
        actual = dset.product(dset).collect()
        actual.sort()
        expected = sorted(product(*[string.ascii_letters] * 2))
        self.assertEqual(actual, expected)


    def test_partition_product(self):
        a = self.ctx.collection(string.ascii_letters, pcount=4)
        b = self.ctx.range(100, pcount=4)

        a_maxes = string.ascii_letters[12::13]
        b_maxes = list(range(24, 100, 25))

        expected = list(product(a_maxes, b_maxes))
        actual = a.product(b, func=lambda a, b: ((max(a), max(b)),)).collect()

        self.assertEqual(expected, actual)
