from statistics import mean

from bndl.compute.dataset.tests import DatasetTest


class SampleTest(DatasetTest):
    def test_sample(self):
        count = 1000000
        dset = self.ctx.range(count, pcount=self.worker_count * 2)
        for fraction in (0.1, 0.5, 0.9):
            sampling = dset.sample(fraction, seed=1)
            sampled = sampling.collect()
            self.assertEqual(sampled, sampling.collect())
            self.assertAlmostEqual(len(sampled) / count, fraction, places=2)
            self.assertAlmostEqual(mean(sampled) / count, 0.4999995, places=2)
