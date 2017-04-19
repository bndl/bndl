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
from bndl.util.funcs import iseven


class GroupbyTest(ComputeTest):
    def test_group_by(self):
        self.assertEqual(self.ctx.range(10).group_by(iseven).map_values(sorted).collect(),
                         [(False, [1, 3, 5, 7, 9]), (True, [0, 2, 4, 6, 8])])

    def test_group_by_key(self):
        dset = self.ctx.range(100).key_by(iseven).group_by_key().map_values(sorted)
        self.assertEqual(dset.collect_as_map(), {
            False: list(range(1, 100, 2)),
            True: list(range(0, 100, 2)),
        })
