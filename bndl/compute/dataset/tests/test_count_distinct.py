from bndl.compute.dataset.tests import DatasetTest
from bndl.util.hyperloglog import HyperLogLog
from bndl.compute.dataset.base import _as_bytes
from random import random


class CountDistinctTest(DatasetTest):
    def test_approx(self):
        for error_rate in (0.01, 0.05, 0.2):
            values = [random() for _ in range(10000)]

            # perform local hll in parallel to verify
            hll = HyperLogLog(error_rate)
            hll.add_all(map(_as_bytes, values))

            for _ in range(5):
                # generate values
                extra = [random() for _ in range(1000)]
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
                hll.add_all(map(_as_bytes, extra))
                self.assertAlmostEqual(count, hll.card())
