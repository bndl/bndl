from bndl.compute.tests import DatasetTest
from cyhll import HyperLogLog
from random import random, randint


class CountDistinctTest(DatasetTest):
    def test_approx(self):
        tvalues = [
            [random() for _ in range(10000)],
            [randint(0, 2 ** 32) for _ in range(10000)],
            ['%i' % randint(0, 2 ** 32) for _ in range(10000)],
            [b'%i' % randint(0, 2 ** 32) for _ in range(10000)],
        ]

        for error_rate in (0.01, 0.05, 0.2):
            for values in tvalues:
                # perform local hll in parallel to verify
                hll = HyperLogLog(error_rate)
                hll.add_all(values)

                for _ in range(2):
                    # generate values
                    extra = [random() for _ in range(941)]
                    values += extra

                    # check count accuracy
                    count = self.ctx.collection(values).count_distinct_approx(error_rate)
                    exact = len(set(values))
                    delta = exact * error_rate * 4
                    self.assertAlmostEqual(count, exact,
                                           delta=delta,
                                           msg='delta %s, error_rate %s' %
                                           (abs(count - exact), error_rate))

                    # check with non distributed counting
                    hll.add_all(extra)
                    self.assertAlmostEqual(count, hll.card())
