from bndl.compute.dataset.tests import DatasetTest


class ShuffleTest(DatasetTest):
    def test_shuffle(self):
        dset = self.ctx.range(10).shuffle(2, lambda i: i % 2)
        parts = sorted(sorted(part) for part in dset.collect(parts=True))
        self.assertEqual(parts, [list(range(0, 10, 2)), list(range(1, 10, 2))])

    # TODO test Dataset.clean
