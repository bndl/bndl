import string

from bndl.compute.dataset.tests import DatasetTest


class CollectionTest(DatasetTest):

    def test_slices(self):
        dset = self.ctx.collection(string.ascii_lowercase)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)


    def test_generator(self):
        g = self.ctx.collection(c for c in string.ascii_lowercase)
        self.assertEqual(g.count(), 26)
        self.assertEqual(g.count(), 26)


    def test_sizing(self):
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, pcount=0)
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, psize=0)
        with self.assertRaises(ValueError):
            self.ctx.collection(string.ascii_lowercase, pcount=1, psize=1)
        self.assertEqual(
            self.ctx
                .collection([1, 2, 3, 4], psize=2)
                .map_partitions_with_index(lambda idx, iterator: [idx])
                .collect(),
            [0, 1]
        )
        self.assertEqual(
            self.ctx
                .collection([1, 2, 3, 4, 5, 6], pcount=3)
                .map_partitions_with_index(lambda idx, iterator: [idx])
                .collect(),
            [0, 1, 2]
        )
