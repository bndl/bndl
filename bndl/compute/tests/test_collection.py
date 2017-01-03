import string

from bndl.compute.tests import DatasetTest


class CollectionTest(DatasetTest):

    def test_slices(self):
        dset = self.ctx.collection(string.ascii_lowercase)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)


    def test_iterator(self):
        it = self.ctx.collection(c for c in string.ascii_lowercase)
        self.assertEqual(it.count(), 26)
        self.assertEqual(it.count(), 26)


    def test_generator(self):
        def gen():
            for c in string.ascii_lowercase:
                yield c
        gen = self.ctx.collection(gen())
        self.assertEqual(gen.count(), 26)
        self.assertEqual(gen.count(), 26)


    def test_dicts(self):
        dct = dict(zip(range(len(string.ascii_lowercase)), string.ascii_lowercase))
        self.assertEqual(dict(self.ctx.collection(dct).collect()), dct)
        self.assertEqual(sorted(self.ctx.collection(dct.keys()).collect()), sorted(dct.keys()))
        self.assertEqual(sorted(self.ctx.collection(dct.values()).collect()), sorted(dct.values()))


    def test_sizing(self):
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, pcount=0)
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, psize=0)
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, pcount=1, psize=1)

        p_indices = self.ctx.collection([1, 2, 3, 4], psize=2) \
                            .map_partitions_with_index(lambda idx, iterator: [idx]) \
                            .collect()
        self.assertEqual(p_indices, [0, 1])

        p_indices = self.ctx.collection([1, 2, 3, 4, 5, 6], pcount=3) \
                            .map_partitions_with_index(lambda idx, iterator: [idx]) \
                            .collect()
        self.assertEqual(p_indices, [0, 1, 2])
