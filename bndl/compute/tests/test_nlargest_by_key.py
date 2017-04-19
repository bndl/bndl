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

from heapq import nlargest, nsmallest
from itertools import groupby, product

from bndl.compute.dataset import Dataset
from bndl.compute.tests import ComputeTest


class LargestByKeyTest(ComputeTest):
    def test_nlargest_by_key(self):
        ks = [3, 7]
        ops = [(nlargest, Dataset.nlargest_by_key),
               (nsmallest, Dataset.nsmallest_by_key)]

        nums = range(100)
        keyf = lambda i:i // 10

        for k, (op, dop) in product(ks, ops):
            expected = {key: list(op(k, values))
                        for key, values in groupby(sorted(nums, key=keyf), keyf)}

            dset = self.ctx.collection(nums).key_by(keyf)
            self.assertEqual(dop(dset, k).collect_as_map(), expected)
