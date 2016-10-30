from bndl.compute.tests import DatasetTest
from bndl.execute.worker import current_worker
from collections import Counter


class SelectWorkersTest(DatasetTest):
    def test_require(self):
        executed_on = self.ctx.accumulator(Counter())
        def register_worker(i):
            nonlocal executed_on
            executed_on += Counter({current_worker().name:1})

        dset = self.ctx.range(10).map(register_worker)

        targeted = Counter()
        for _ in range(2):
            for worker in self.ctx.workers:
                worker_name = worker.name
                targeted[worker_name] += 10
                dset.require_workers(lambda w: w.name == worker_name).execute()
                self.assertEqual(executed_on.value, targeted)
