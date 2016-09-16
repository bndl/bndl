from bndl.compute.tests import DatasetTest


class ShuffleTest(DatasetTest):
    def test_shuffle(self):
        size = 100*1000
        
        dset = self.ctx.range(size, pcount=3).shuffle(pcount=3, partitioner=lambda i: i % 2)
        parts = sorted(sorted(part) for part in dset.collect(parts=True) if part)
        
        self.assertEqual(parts, [list(range(0, size, 2)), list(range(1, size, 2))])

    # TODO test Dataset.clean
