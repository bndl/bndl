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


class KeyByIdTest(DatasetTest):
    def test_key_by_id(self):
        with_id = self.ctx.range(10000).key_by_id()
        self.assertEqual(with_id.count(), with_id.keys().count_distinct())

    def test_key_by_idx(self):
        with_idx = self.ctx.range(10000).key_by_idx()
        self.assertEqual(with_idx.keys().collect(), with_idx.values().collect())
