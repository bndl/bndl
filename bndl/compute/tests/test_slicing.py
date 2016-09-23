import string

from bndl.compute.tests import DatasetTest


class SlicingTest(DatasetTest):
    def test_limit(self):
        dset = self.ctx.collection(string.ascii_lowercase)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)

        dset = self.ctx.collection(string.ascii_lowercase, pcount=100)
        self.assertEqual(dset.first(), string.ascii_lowercase[0])
        self.assertEqual(''.join(dset.take(10)), string.ascii_lowercase[:10])
        self.assertEqual(''.join(dset.collect()), string.ascii_lowercase)

    def test_next(self):
        print(next(self.ctx.range(10).icollect(ordered=False)))