import copy
import gc
import itertools
import random

from bndl.compute import cache
from bndl.compute.tests import DatasetTest


class CachingTest(DatasetTest):
    worker_count = 3

    def test_caching(self):
        dset = self.ctx.range(10).map(lambda i: random.randint(1, 1000)).map(str)

        locations = ('memory', 'disk')
        serializations = (None, 'marshal', 'pickle', 'json', 'text', 'binary')
        compressions = (None, 'gzip')

        options = itertools.product(locations, serializations, compressions)
        for location, serialization, compression in options:
            if not serialization and (location == 'disk' or compression):
                continue
            self.caching_subtest(dset, location, serialization, compression)


    def caching_subtest(self, dset, location, serialization, compression):
        dset = copy.copy(dset)
        if serialization == 'binary':
            dset = dset.map(str.encode)
        params = dict(location=location, serialization=serialization, compression=compression)
        with self.subTest('Caching subtest', **params):
            self.assertNotEqual(dset.collect(), dset.collect())
            self.assertEqual(self.get_cachekeys(), [])

            dset.cache(**params)

            self.assertEqual(dset.collect(), dset.collect())
            self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.worker_count)

            dset.cache(False)
            self.assertNotEqual(dset.collect(), dset.collect())
            self.assertEqual(self.get_cachekeys(), [])

            dset.cache(**params)
            # check again, a) to check whether a dataset can be 'recached'
            # and b) with a transformation to test caching a dataset 'not at the end'
            self.assertEqual(dset.map(lambda i: i).collect(), dset.map(lambda i: i).collect())
            self.assertEqual(self.get_cachekeys(), [dset.id] * self.ctx.worker_count)

            del dset
            for _ in range(3):
                gc.collect()
            self.assertEqual(self.get_cachekeys(), [])


    def get_cachekeys(self):
        def get_keys(p, i):
            try:
                return list(cache._caches.keys())
            except KeyError:
                return []
        return self.ctx.range(self.worker_count) \
                   .map_partitions_with_part(get_keys) \
                   .collect()
