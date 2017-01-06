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

from collections import Counter
from operator import add

from bndl.compute.tests import DatasetTest


class TreeAggregateTest(DatasetTest):
    def setUp(self):
        super().setUp()
        self.dset = self.ctx.range(1000, pcount=100)

    def test_aggregate(self):
        self.assertEqual(self.dset.tree_aggregate(sum), self.dset.sum())
        self.assertEqual(self.dset.tree_aggregate(Counter, lambda counters: sum(counters, Counter())),
                         Counter(self.dset.collect()))

    def test_reduce(self):
        self.assertEqual(self.dset.tree_reduce(add), self.dset.sum())
        self.assertEqual(self.dset.map_partitions(Counter).glom().tree_reduce(add),
                         self.dset.count_by_value())

    def test_combine(self):
        def count(counter, value):
            counter[value] += 1
            return counter
        self.assertEqual(self.dset.tree_combine(Counter(), count, add),
                         Counter(self.dset.collect()))

    def test_depth(self):
        for d in range(2, 5):
            self.assertEqual(self.dset.tree_aggregate(sum, depth=d),
                             self.dset.sum())
