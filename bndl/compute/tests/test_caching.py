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
from bndl.compute.tests import DatasetTest
from bndl.execute.worker import current_worker
from bndl.util.funcs import identity


class CachingTest(DatasetTest):
    worker_count = 3


    def get_cachekeys(self):
        fetches = [worker.service('tasks').execute(lambda: list(cache._caches.keys()))
                   for worker in self.ctx.workers]
        return list(chain.from_iterable(fetch.result() for fetch in fetches))


    def gc_collect(self):
        for _ in range(3):
            gc.collect()
            time.sleep(0)
        time.sleep(1)


    def test_caching(self):
        dset = self.ctx.range(10, pcount=3).map(lambda i: random.randint(1, 1000)).map(str)

        locations = ('memory', 'disk')
        serializations = (None, 'marshal', 'pickle', 'json', 'text', 'binary')
        compressions = (None, 'gzip')

        options = itertools.product(locations, serializations, compressions)
        for location, serialization, compression in options:
            if not serialization and (location == 'disk' or compression):
                continue
            self.caching_subtest(dset, location, serialization, compression)


    def caching_subtest(self, dset, location, serialization, compression):
        self.assertEqual(self.get_cachekeys(), [])

        dset = dset.map(identity)
        if serialization == 'binary':
            dset = dset.map(str.encode)
        params = dict(location=location, serialization=serialization, compression=compression)

        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache(**params)

        self.assertEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.worker_count)

        dset.uncache(True, 5)
        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache(**params)
        # check again, a) to check whether a dataset can be 'recached'
        # and b) with a transformation to test caching a dataset 'not at the end'
        self.assertEqual(dset.map(lambda i: i).collect(), dset.map(lambda i: i).collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.worker_count)

        del dset
        self.gc_collect()
        self.assertEqual(self.get_cachekeys(), [])


    def test_cache_fetch(self):
        self.assertEqual(self.get_cachekeys(), [])

        executed_on = self.ctx.accumulator(Counter())
        def register_worker(i):
            nonlocal executed_on
            executed_on += Counter({current_worker().name:1})

        dset = self.ctx.range(10, pcount=3).map(lambda i: random.randint(1, 1000)).map(str).cache()
        w0, w1 = (w.name for w in self.ctx.workers[0:2])

        first = dset.map(register_worker).require_workers(lambda workers: [w for w in workers if w.name == w0]).execute()
        self.assertEqual(executed_on.value, Counter({w0:10}))
        self.assertEqual(self.get_cachekeys(), [dset.id])

        second = dset.map(register_worker).require_workers(lambda workers: [w for w in workers if w.name == w1]).execute()
        self.assertEqual(executed_on.value, Counter({w0:10, w1:10}))
        self.assertEqual(self.get_cachekeys(), [dset.id])

        self.assertEqual(first, second)

        del dset
        self.gc_collect()
        self.assertEqual(self.get_cachekeys(), [])
