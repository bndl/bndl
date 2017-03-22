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

from unittest.case import TestCase

from bndl.util.collection import batch


class BatchingTest(TestCase):
    def test_batch_with_step(self):
        res = []
        for b in batch(list(range(10)), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_range(self):
        res = []
        for b in batch(range(10), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_iterator(self):
        res = []
        for b in batch(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_generator(self):
        res = []
        for b in batch((i for i in range(10)), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))
