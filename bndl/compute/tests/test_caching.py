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
from itertools import chain
import gc
import itertools
import random
import time

from bndl.compute import cache
from bndl.compute.tasks import current_node
from bndl.compute.tests import ComputeTest
from bndl.util.funcs import identity


class CachingTest(ComputeTest):
    def get_cachekeys(self):
        fetches = [e.service('tasks').execute(lambda: list(cache._caches.keys()))
                   for e in self.ctx.executors]
        return list(chain.from_iterable(fetch.result() for fetch in fetches))


    def gc_collect(self):
        for i in range(3):
            gc.collect(i)
            time.sleep(0)


    @classmethod
    def _setup_tests(cls):
        locations = ('memory', 'disk')
        serializations = (None, 'marshal', 'pickle', 'json', 'text', 'binary')
        compressions = (None, 'gzip', 'lz4')

        cases = itertools.product(locations, serializations, compressions)

        for case in cases:
            _, serialization, compression = case
            if compression and not serialization:
                continue
            setattr(
                cls,
                'test_caching_' + '_'.join(map(lambda o: str(o).lower(), case)),
                lambda self, case=case: self._test_caching(*case)
            )


    def _test_caching(self, location, serialization, compression):
        dset = self.ctx.range(10, pcount=3).map(lambda i: random.randint(1, 1000)).map(str)

        self.assertEqual(self.get_cachekeys(), [])

        dset = dset.map(identity)
        if serialization == 'binary':
            dset = dset.map(str.encode)
        params = dict(location=location, serialization=serialization, compression=compression)

        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache(**params)

        self.assertEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.executor_count)

        dset.uncache(True, 5)
        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache(**params)
        # check again, a) to check whether a dataset can be 'recached'
        # and b) with a transformation to test caching a dataset 'not at the end'
        self.assertEqual(dset.map(lambda i: i).collect(), dset.map(lambda i: i).collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.executor_count)

        del dset
        self.gc_collect()
        self.assertEqual(self.get_cachekeys(), [])


    def test_cache_fetch(self):
        self.assertEqual(self.get_cachekeys(), [])

        executed_on = self.ctx.accumulator(Counter())
        def register_executor(i):
            nonlocal executed_on
            executed_on += Counter({current_node().name:1})

        dset = self.ctx.range(10, pcount=3).map(lambda i: random.randint(1, 1000)).map(str).cache()
        e0, e1 = (e.name for e in self.ctx.executors[0:2])

        first = dset.map(register_executor).require_executors(lambda executors: [e for e in executors if e.name == e0]).execute()
        self.assertEqual(executed_on.value, Counter({e0:10}))
        self.assertEqual(self.get_cachekeys(), [dset.id])

        second = dset.map(register_executor).require_executors(lambda executors: [e for e in executors if e.name == e1]).execute()
        self.assertEqual(executed_on.value, Counter({e0:10, e1:10}))
        self.assertEqual(self.get_cachekeys(), [dset.id])

        self.assertEqual(first, second)

        del dset
        self.gc_collect()
        self.assertEqual(self.get_cachekeys(), [])


CachingTest._setup_tests()
