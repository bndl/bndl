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

from statistics import mean

from bndl.compute.tests import ComputeTest


class SampleTest(ComputeTest):
    def test_sample(self):
        count = 1000000
        dset = self.ctx.range(count, pcount=self.executor_count * 2)
        for fraction in (0.1, 0.5, 0.9):
            sampling = dset.sample(fraction, seed=1)
            sampled = sampling.collect()
            self.assertEqual(sampled, sampling.collect())
            self.assertAlmostEqual(len(sampled) / count, fraction, places=2)
            self.assertAlmostEqual(mean(sampled) / count, 0.4999995, places=2)
