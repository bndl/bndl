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

import string

from bndl.compute.tests import DatasetTest


class TakeOrderedTest(DatasetTest):

    def test_nokey(self):
        dset = self.ctx.collection(string.ascii_lowercase).shuffle()
        self.assertEqual(''.join(dset.nlargest(2)), string.ascii_lowercase[:-3:-1])
        self.assertEqual(''.join(dset.nlargest(10)), string.ascii_lowercase[:-11:-1])
        self.assertEqual(''.join(dset.nsmallest(2)), string.ascii_lowercase[:2])
        self.assertEqual(''.join(dset.nsmallest(10)), string.ascii_lowercase[:10])


    def test_withkey(self):
        dset = self.ctx.range(100).shuffle()
        self.assertEqual(dset.nlargest(10, key=lambda i:-i), list(range(10)))
        self.assertEqual(dset.nsmallest(10, key=lambda i:-i), list(range(99, 89, -1)))
        self.assertEqual(dset.nsmallest(10, key=str), sorted(range(100), key=str)[:10])
