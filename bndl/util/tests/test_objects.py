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
from functools import partial
from unittest.case import TestCase
import pickle

from bndl.util.objects import LazyObject


class Expensive(object):
    def __init__(self, counters):
        self.counters = counters
        self.counters['created'] += 1
        self.x = '1'

    def stop(self):
        self.counters['destroyed'] += 1

    def __eq__(self, other):
        return isinstance(other, Expensive) and self.x == other.x


class TestLazyObject(TestCase):
    def setUp(self):
        self.counters = Counter()
        self.l = LazyObject(partial(Expensive, self.counters), destructor='stop')


    def test_factory(self):
        self.assertEqual(self.l.x, '1')


    def test_destructor(self):
        self.assertEqual(self.counters['created'], 0)
        self.assertEqual(self.counters['destroyed'], 0)

        self.assertEqual(self.l.x, '1')
        self.assertEqual(self.counters['created'], 1)
        self.assertEqual(self.counters['destroyed'], 0)

        self.l.stop()
        self.assertEqual(self.counters['created'], 1)
        self.assertEqual(self.counters['destroyed'], 1)

        self.assertEqual(self.l.x, '1')
        self.assertEqual(self.counters['created'], 2)
        self.assertEqual(self.counters['destroyed'], 1)


    def test_pickle(self):
        p = pickle.dumps(self.l)
        l2 = pickle.loads(p)
        self.assertEqual(self.l, l2)
