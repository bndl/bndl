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

from bndl.compute.tests import ComputeTest


class CoalescePartsTest(ComputeTest):
    def test_range(self):
        data = list(range(100))
        dset = self.ctx.collection(data, pcount=10)

        for pcount in range(10):
            pcount += 1
            coalesced = dset.coalesce_parts(pcount)
            self.assertEqual(coalesced.pcount, pcount)
            self.assertEqual(data, coalesced.collect())

        for pcount in (11, 13, 100):
            self.assertEqual(dset.coalesce_parts(pcount), dset)

        with self.assertRaises(AssertionError):
            for pcount in (-10, -1, 0):
                dset.coalesce_parts(pcount)
