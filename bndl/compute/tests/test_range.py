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


class RangeTest(ComputeTest):
    def setUp(self):
        self.dset = self.ctx.range(1000)

    def test_get_first(self):
        self.assertEqual(self.dset.first(), 0)

    def test_take_10(self):
        self.assertEqual(self.dset.take(10), list(range(10)))

    def test_collect(self):
        self.assertEqual(self.dset.collect(), list(range(1000)))

    def test_icollect(self):
        self.assertEqual(list(self.dset.icollect()), list(range(1000)))

    def test_as_collection(self):
        self.assertEqual(self.ctx.collection(range(1000)).collect(), list(range(1000)))
