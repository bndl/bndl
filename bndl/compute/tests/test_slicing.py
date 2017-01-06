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


class SlicingTest(DatasetTest):
    def test_limit(self):
        dset = self.ctx.collection(string.ascii_lowercase)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)

        dset = self.ctx.collection(string.ascii_lowercase, pcount=100)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)

    def test_next(self):
        print(next(self.ctx.range(10).icollect(ordered=False)))
