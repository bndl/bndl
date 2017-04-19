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

from random import random, randint
import sys

from cyhll import HyperLogLog

from bndl.compute.tests import ComputeTest


class CountDistinctTest(ComputeTest):
    def test_approx(self):
        tvalues = [
            [random() * 2 ** 128 for _ in range(10000)],
            [randint(0, 2 ** 128) for _ in range(10000)],
            ['%i' % randint(0, 2 ** 128) for _ in range(10000)],
            [('%i' % randint(0, 2 ** 128)).encode() for _ in range(10000)],
        ]

        for error_rate in (0.01, 0.05, 0.2):
            for values in tvalues:
                # perform local hll in parallel to verify
                hll = HyperLogLog(error_rate)
                hll.add_all(values)

                for _ in range(2):
                    # generate values
                    extra = [random() for _ in range(941)]
                    values += extra

                    # check count accuracy
                    count = self.ctx.collection(values).count_distinct_approx(error_rate)
                    exact = len(set(values))
                    delta = exact * error_rate * 4
                    self.assertAlmostEqual(count, exact,
                                           delta=delta,
                                           msg='delta %s, error_rate %s' %
                                           (abs(count - exact), error_rate))

                    # check with non distributed counting
                    hll.add_all(extra)
                    self.assertAlmostEqual(count, hll.card())
