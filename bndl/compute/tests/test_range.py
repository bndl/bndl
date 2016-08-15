from bndl.compute.tests import DatasetTest


class RangeTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(1000)

    def test_get_first(self):
        self.assertEqual(self.dset.first(), 0)

    def test_take_10(self):
        self.assertEqual(self.dset.take(10), list(range(10)))

    def test_collect(self):
        self.assertEqual(self.dset.collect(), list(range(1000)))

    def test_icollect(self):
        self.assertEqual(list(self.dset.icollect()), list(range(1000)))

    def test_as_collection(self):
        self.assertEqual(self.ctx.collection(range(1000)).collect(), list(range(1000)))
