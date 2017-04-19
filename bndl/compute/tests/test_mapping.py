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

from itertools import chain
import operator

from bndl.compute.tests import ComputeTest
from bndl.util.funcs import identity


class MappingTest(ComputeTest):
    def setUp(self):
        super().setUp()
        self.dset = self.ctx.range(10, pcount=3)

    def test_mapping(self):
        remainders = self.dset.map(lambda x: x % 2)
        self.assertEqual(remainders.collect(), list(x % 2 for x in range(10)))
        remainders = remainders.map(str)
        self.assertEqual(remainders.collect(), list(str(x % 2) for x in range(10)))

    def test_flatmap(self):
        self.assertEqual(self.dset.flatmap(lambda i: (i,)).collect(),
                         list(range(10)))
        self.assertEqual(self.dset.flatmap(lambda x: [x, x + 1]).collect(),
                        list(chain.from_iterable((x, x + 1) for x in range(10))))
        self.assertEqual(''.join(self.ctx.range(100, pcount=3).map(str).flatmap(identity).collect()),
                         ''.join(list(map(str, range(100)))))

    def test_map_partitions(self):
        self.assertEqual(self.dset.map_partitions(identity).collect(), list(range(10)))
        self.assertEqual(sorted(self.dset.map_partitions(lambda p: (len(p),)).collect()), [3, 3, 4])
        self.assertEqual(self.dset.map_partitions_with_index(lambda idx, i: (idx,)).collect(), [0, 1, 2])
        self.assertEqual(self.dset.map_partitions_with_part(lambda p, it: (p.idx,)).collect(), [0, 1, 2])

    def test_extra_args(self):
        self.assertEqual(self.dset.map(lambda x, i: x + i, 1).sum(), 10 + 45)
        self.assertEqual(self.dset.flatmap(lambda x, i: (1, x), 1).count(), 10 + 10)
        self.assertEqual(self.dset.key_by(identity).starmap(lambda x, i1, i2: x + i1 + i2, 1).sum(), 10 + 45 + 45)
        self.assertEqual(self.dset.filter(operator.le, 5).sum(), 5 + 6 + 7 + 8 + 9)
        self.assertEqual(self.dset.map_partitions(lambda x, p: [x], 5).sum(), 3 * 5)
        self.assertEqual(self.dset.map_partitions_with_index(lambda x, idx, p: [x + idx], 5).sum(), 3 * 5 + 0 + 1 + 2)
        self.assertEqual(self.dset.map_partitions_with_part(lambda x, part, p: [x + part.idx], 5).sum(), 3 * 5 + 0 + 1 + 2)
