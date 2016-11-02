import os
import signal
import threading
import time

from bndl.compute.tests import DatasetTest
from bndl.net.connection import NotConnected
from cytoolz.itertoolz import pluck


class ShuffleTest(DatasetTest):
    worker_count = 15

    # TODO test Dataset.clean

    def test_shuffle(self):
        size = 100 * 1000

        dset = self.ctx.range(size, pcount=30).shuffle(sort=False).shuffle(pcount=3, partitioner=lambda i: i % 2)
        parts = sorted(sorted(part) for part in dset.collect(parts=True) if part)
        self.assertEqual(parts, [list(range(0, size, 2)), list(range(1, size, 2))])


    def test_dependency_failure(self):
        class WorkerKiller(object):
            def __init__(self, worker, count):
                self.count = count + 1
                self.worker = worker
                self.lock = threading.Lock()

            def countdown(self, i):
                with self.lock:
                    self.count -= i
                    if self.count == 0:
                        try:
                            self.worker.run_task(lambda: os.kill(os.getpid(), signal.SIGKILL)).result()
                        except NotConnected:
                            pass

            def __str__(self):
                return 'kill %s after %s elements' % (self.worker.name, self.count)

        def identity_mapper(killers, key_count, i):
            for killer in killers:
                killer.update('countdown', 1)
            return i

        def keyby_mapper(killers, key_count, i):
            identity_mapper(killers, key_count, i)
            return (i % key_count, i)

        # dset_size, pcount, key_count, killers
        test_cases = [
            [100, 30, 30, [75, 100, 125]],
            [100, 30, 30, [100, 100, 100]],
            [100, 30, 30, [0, 50, 100]],
            [ 10, 3, 3, [10]],
            [ 10, 3, 3, [0]],
        ]

        try:
            for dset_size, pcount, key_count, kill_after in test_cases:
                print(dset_size, pcount, key_count, *kill_after)

                with self.subTest('dset_size = %r, pcount = %r, key_count = %r, kill_after = %r' \
                                  % (dset_size, pcount, key_count, kill_after)):
                    self.ctx.conf['bndl.execute.attempts'] = 10 * len(kill_after)

                    killers = [
                        self.ctx.accumulator(WorkerKiller(worker, count))
                        for worker, count in zip(self.ctx.workers, kill_after)
                    ]

                    dset = self.ctx \
                               .range(dset_size, pcount=pcount) \
                               .map(identity_mapper, killers, key_count) \
                               .shuffle() \
                               .map(keyby_mapper, killers, key_count) \
                               .aggregate_by_key(sum)

                    result = dset.collect()
                    self.assertEqual(len(result), key_count)
                    self.assertEqual(sorted(pluck(0, result)), list(range(key_count)))
                    self.assertEqual(sum(pluck(1, result)), sum(range(dset_size)))

                    time.sleep(1)
                    print('-' * 80)
        finally:
            self.ctx.conf['bndl.execute.attempts'] = 1
