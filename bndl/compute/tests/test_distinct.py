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

from bndl.compute.tests import DatasetTest
from bndl.util.funcs import iseven, isodd


class TestDistinct(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(1000, pcount=3)

    def test_distinct_odd_even(self):
        remainders = sorted(self.dset.map(lambda x: x % 2).distinct().collect())
        self.assertEqual(remainders, [0, 1])

    def test_collect_as_set(self):
        distinct = self.dset.map(lambda x:x % 2).distinct()
        remainders = distinct.collect_as_set()
        self.assertEqual(remainders, {0, 1})
        self.assertEqual(list(remainders), distinct.collect())

    def test_distinct_by_key(self):
        remainders = self.dset.distinct(3, key=lambda x: x % 2).collect()
        self.assertEqual(len(remainders), 2)
        remainders.sort(key=iseven)
        self.assertTrue(isodd(remainders[0]))
        self.assertTrue(iseven(remainders[1]))
