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

import random

from bndl.compute.tests import DatasetTest
from bndl.util.funcs import getter, identity


class SortTest(DatasetTest):
    def test_sort(self):
        for length in (10, 1000):
            for maxint in (3, 10, 100, 1000):
                col = [random.randint(1, maxint) for _ in range(length)]
                dset = self.ctx.collection(col)
                self.assertEqual(dset.sort().collect(), sorted(col))


    def test_sort_by_key(self):
        length = 100
        maxint = 100
        col = [random.randint(1, maxint) for _ in range(length)]
        dset = self.ctx.collection(col).key_by_id().map(list)
        self.assertEqual(dset.sort(key=getter(1)).values().collect(), sorted(col))


    def test_sort_unhashable(self):
        length = 100
        maxint = 100
        col = [random.randint(1, maxint) for _ in range(length)]
        dset = self.ctx.collection(col).key_by(identity).map(list)
        self.assertEqual(dset.sort().values().collect(), sorted(col))
