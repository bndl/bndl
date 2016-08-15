import string

from bndl.compute.tests import DatasetTest


class TakeOrderedTest(DatasetTest):

    def test_nokey(self):
        dset = self.ctx.collection(string.ascii_lowercase).shuffle()
        self.assertEqual(''.join(dset.nlargest(2)), string.ascii_lowercase[:-3:-1])
        self.assertEqual(''.join(dset.nlargest(10)), string.ascii_lowercase[:-11:-1])
        self.assertEqual(''.join(dset.nsmallest(2)), string.ascii_lowercase[:2])
        self.assertEqual(''.join(dset.nsmallest(10)), string.ascii_lowercase[:10])


    def test_withkey(self):
        dset = self.ctx.range(100).shuffle()
        self.assertEqual(dset.nlargest(10, key=lambda i:-i), list(range(10)))
        self.assertEqual(dset.nsmallest(10, key=lambda i:-i), list(range(99, 89, -1)))
        self.assertEqual(dset.nsmallest(10, key=str), sorted(range(100), key=str)[:10])
