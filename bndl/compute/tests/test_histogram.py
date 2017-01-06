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

from random import random

from bndl.compute.tests import DatasetTest
import numpy as np


class HistogramTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.collection([int(random() * 100) for _ in range(1000)])

    def test_binning(self):
        bins = (
            [0, 10, 25, 50, 100],
            [0, 2, 3, 4, 5],
        )
        for bins in bins:
            self.assertHistogramsEqual(self.dset.histogram(bins), np.histogram(self.dset.collect(), bins))

    def test_autobinning(self):
        for bins in (10, 20, 50, 100, 1000):
            self.assertHistogramsEqual(self.dset.histogram(bins), np.histogram(self.dset.collect(), bins))

    def assertHistogramsEqual(self, a, b):
        hista, binsa = a
        histb, binsb = b
        self.assertTrue(np.allclose(hista, histb))
        self.assertTrue(np.allclose(binsa, binsb))
