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
from unittest.case import TestCase
import pickle
import time

from bndl.util.pool import ObjectPool


class ObjectPoolTest(TestCase):
    def setUp(self):
        self.created = 0
        self.checked = Counter()
        self.destroyed = Counter()
        self.poisoned = set()


    def _create(self):
        new = self.created
        self.created += 1
        return new


    def _check(self, obj):
        self.checked[obj] += 1
        return False if obj in self.poisoned else True


    def _destroy(self, obj):
        self.destroyed[obj] += 1


    def test_normal(self):
        pool = ObjectPool(self._create, self._check, self._destroy)
        self.assertEqual(self.created, 0)
        for _ in range(10):
            first = pool.get()
            self.assertEqual(first, 0)
            self.assertEqual(self.created, 1)
            pool.put(first)
            second = pool.get()
            self.assertEqual(second, first)
            self.assertEqual(self.created, 1)
            self.assertEqual(len(self.checked), 1)
            self.assertEqual(len(self.destroyed), 0)
            pool.put(second)

        for _ in range(10):
            objs = [pool.get() for _ in range(10)]
            self.assertEqual(objs, list(range(10)))
            for obj in objs:
                pool.put(obj)
            self.assertEqual(len(self.checked), 10)
            self.assertEqual(len(self.destroyed), 0)

        self.poisoned.add(3)
        objs = [pool.get() for _ in range(10)]
        self.assertEqual(objs, [i for i in range(11) if i != 3])
        self.assertNotIn(3, objs)
        self.assertEqual(len(self.destroyed), 1)
        self.assertEqual(self.destroyed[3], 1)


    def test_pickle(self):
        pool = ObjectPool(self._create, self._check, self._destroy, min_size=10)
        self.assertEqual(self.created, 10)
        self.assertEqual(pool.objects.qsize(), 10)

        objs = [pool.get() for _ in range(10)]
        self.assertEqual(objs, list(range(10)))
        for obj in objs:
            pool.put(obj)
        self.assertEqual(pool.objects.qsize(), 20)

        del pool.factory
        del pool.check
        del pool.destroy
        pool2 = pickle.loads(pickle.dumps(pool))
        pool2.factory = self._create
        pool2.check = self._check
        pool2.destroy = self._destroy

        self.assertEqual(pool2.objects.qsize(), 20)
        objs = [pool2.get() for _ in range(10)]
        self.assertEqual(objs, list(range(10, 20)))
        self.assertEqual(self.created, 20)


    def test_max_age(self):
        pool = ObjectPool(self._create, self._check, self._destroy, min_size=1, max_idle=.1)
        sentinel = object()
        pool.put(sentinel)
        pool.get()

        first = pool.get()
        self.assertEqual(first, sentinel)
        self.assertEqual(self.created, 2)
        pool.put(first)

        time.sleep(.1)
        second = pool.get()
        self.assertNotEqual(first, second)
        self.assertEqual(second, 2)

        third = pool.get()
        self.assertEqual(third, 3)
        self.assertTrue(self.created >= 4)

        created_last = self.created
        for _ in range(3):
            time.sleep(.2)
            self.assertTrue(self.created > created_last)
            created_last = self.created
