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

from bndl.compute.stats import MultiVariateStats, Stats
from bndl.compute.tests import DatasetTest
import numpy as np


statistics = [
    'mean', 'min', 'max',
    'variance', 'sample_variance',
    'stdev', 'sample_stdev',
    'skew', 'kurtosis',
]

width = 5

arrs = [
    [np.random.random((100, width)), np.random.random((150, width))],
    [np.zeros((100, width)), np.zeros((150, width))],
    [np.ones((100, width)), np.ones((150, width))],
    [np.empty((0, width))] * 2
]



class MvStatsTest(DatasetTest):
    def test_local(self):
        for arr_a, arr_b in arrs:
            mvs_a = MultiVariateStats(width, arr_a)
            mvs_b = MultiVariateStats(width, arr_b)
            mvs_c = mvs_a + mvs_b

            self.assertEqual(mvs_c.count, len(arr_a) + len(arr_b))

            for i in range(width):
                ss_a = Stats(arr_a[:, i])
                ss_b = Stats(arr_b[:, i])
                ss_c = ss_a + ss_b

                for statistic in statistics:
                    stat_a = getattr(mvs_c, statistic)[i]
                    stat_b = getattr(ss_c, statistic)

                    if np.isnan(stat_a) and np.isnan(stat_b):
                        continue
                    self.assertTrue(np.isclose(stat_a, stat_b),
                                    '%s for column %s is not close (%s, %s)'
                                    % (statistic, i, stat_a, stat_b))


    def assert_mvs_close(self, a, b):
        for statistic in statistics:
            stat_a = getattr(a, statistic)
            stat_b = getattr(b, statistic)
            if np.all(np.isnan(stat_a)) and np.all(np.isnan(stat_b)):
                continue
            self.assertTrue(np.allclose(stat_a, stat_b), '%s is not close (%s, %s)' % (statistic, stat_a, stat_b))


    def test_distributed_arrays(self):
        for arr_a, arr_b in arrs:
            arr = np.vstack((arr_a, arr_b))
            if len(arr) == 0:
                continue
            dmvs = self.ctx.collection(arr).mvstats()
            mvs = MultiVariateStats(width, arr)
            self.assert_mvs_close(dmvs, mvs)


    def test_other_types(self):
        arr = self.ctx.collection(np.random.uniform(0, 1, (10, width)))
        expected = arr.mvstats()
        self.assert_mvs_close(expected, arr.map(tuple).mvstats(width))
        self.assert_mvs_close(expected, arr.map(list).mvstats(width))
        self.assert_mvs_close(expected, arr.map(lambda v: (e for e in v)).mvstats(width))
