import gc
import random

from bndl.compute.tests import DatasetTest


class CachingTest(DatasetTest):
    def test_memory_cache(self):
        dset = self.ctx.range(10 * 1000).map(lambda i: random.randint(1, 1000))
        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache()
        self.assertEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * 4)

        dset.cache(False)
        self.assertNotEqual(dset.collect(), dset.collect())
        self.assertEqual(self.get_cachekeys(), [])

        dset.cache()
        # check again, a) to check whether a dataset can be 'recached'
        # and b) with a transformation to test caching a dataset 'not at the end'
        self.assertEqual(dset.map(lambda i: i).collect(), dset.map(lambda i: i).collect())
        self.assertEqual(self.get_cachekeys(), [dset.id] * 4)

        del dset
        for _ in range(3):
            gc.collect()
        self.assertEqual(self.get_cachekeys(), [])


    def get_cachekeys(self):
        def get_keys(p, i):
            return list(p.dset.ctx.node.dset_cache.keys())
        return self.ctx.range(self.worker_count) \
                   .map_partitions_with_part(get_keys) \
                   .collect()
