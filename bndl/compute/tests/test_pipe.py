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

import time

from bndl.compute.tests import DatasetTest
from bndl.rmi import InvocationException


class PipeTest(DatasetTest):
    def test_sort(self):
        lines = [
            'abc\n',
            'zxyv\n',
            'anaasdf \n',
        ]
        self.assertEqual(sorted(self.ctx.collection(lines, pcount=1).map(str.encode).pipe('sort').map(bytes.decode).collect()),
                         sorted(lines))


    def test_failure(self):
        with self.assertRaises(InvocationException):
            (self.ctx.range(3)
                     .map(lambda i: time.sleep(1) or exec('raise Exception()'))
                     .pipe('sort').collect()
            )
