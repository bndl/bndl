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


class FilterTest(ComputeTest):
    def test_filter_lamba(self):
        self.assertEqual(self.ctx.range(1, 100).filter(lambda i: i % 2).count(), 50)

    def test_filter_bool(self):
        self.assertEqual(self.ctx.range(1, 100).map(lambda i: i % 2).filter().count(), 50)

    def test_starfilter(self):
        self.assertEqual(self.ctx.range(1, 100).key_by_id().starfilter(lambda a, b: b >= 50).values().sum(), 4950 - 1225)
