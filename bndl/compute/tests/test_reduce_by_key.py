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

from collections import Counter
from operator import add

from bndl.compute.tests import ComputeTest
from bndl.util import strings


class ReduceByKeyTest(ComputeTest):
    def test_wordcount(self):
        words = [strings.random(2) for _ in range(100)] * 5
        counts = Counter(words)
        dset = self.ctx.collection(words, pcount=4).with_value(1).reduce_by_key(add)
        self.assertEqual(dset.count(), len(counts))
        for word, count in dset.collect():
            self.assertTrue(word in counts)
            self.assertEqual(count, counts[word])
