from bndl.compute.dataset.tests import DatasetTest


class SampleTest(DatasetTest):
    def test_sample(self):
        for fraction in (0.01, 0.5, 0.9):
            dset = self.ctx.range(1000000, pcount=100).map(lambda v: v % 2)
            sampled = dset.sample(fraction, seed=1)
            self.assertAlmostEqual(sampled.count() / dset.count(), fraction, places=1)
            self.assertAlmostEqual(sampled.mean(), dset.mean(), places=1)
