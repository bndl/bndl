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

from itertools import groupby
from operator import itemgetter

from bndl.compute.tests import DatasetTest
from cytoolz.itertoolz import pluck


class ReduceByKeyTest(DatasetTest):
    def test_average(self):
        values = range(100)
        keys = list(map(lambda i: i // 20, values))

        expected = {}
        for key, group in groupby(zip(keys, values), itemgetter(0)):
            vals = list(pluck(1, group))
            expected[key] = sum(vals) / len(vals)

        pairs = self.ctx.collection(zip(keys, values))
        sum_count = pairs.combine_by_key(lambda value: (value, 1),
                                         lambda x, value: (x[0] + value, x[1] + 1),
                                         lambda x, y: (x[0] + y[0], x[1] + y[1]))
        avg_by_key = sum_count.starmap(lambda key, value: (key, value[0] / value[1]))

        self.assertDictEqual(avg_by_key.collect_as_map(), expected)
