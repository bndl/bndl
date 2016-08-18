import gc
import random

from bndl.compute import cache
from bndl.compute.cache import InMemory, SerializedInMemory, OnDisk
from bndl.compute.tests import DatasetTest
from bndl.util.funcs import identity


class CachingTest(DatasetTest):
    worker_count = 3

    def test_caching(self):
        for location in ('memory', 'disk'):
            for serialization in (None, 'marshal', 'pickle', 'json'):
                for compression in (None, 'gzip'):
                    if not serialization and (location == 'disk' or compression):
                        continue
                    self.caching_subtest(location, serialization, compression)

    def caching_subtest(self, location, serialization, compression):
        params = dict(location=location, serialization=serialization, compression=compression)
        dset = self.ctx.range(1000).map(lambda i: random.randint(1, 1000))
        with self.subTest('Caching subtest', **params):
            self.assertNotEqual(dset.collect(), dset.collect())
            self.assertEqual(self.get_cachekeys(), [])

            dset.cache(**params)

            provider = dset._cache_provider
            if not serialization:
                self.assertEqual(provider.serialize, None)
            else:
                self.assertTrue(serialization in provider.serialize.__module__,
                                '%r not in %r' % (serialization, provider.serialize.__module__))
            if compression:
                self.assertIsNotNone(provider.io_wrapper)
            else:
                self.assertEqual(provider.io_wrapper, identity)
            if location == 'memory':
                self.assertIn(provider.holder_cls, (InMemory, SerializedInMemory))
            else:
                self.assertEqual(provider.holder_cls, (OnDisk))

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
                return list(cache._caches[p.dset.ctx.node.name].keys())
            except KeyError:
                return []
        return self.ctx.range(self.worker_count) \
                   .map_partitions_with_part(get_keys) \
                   .collect()
