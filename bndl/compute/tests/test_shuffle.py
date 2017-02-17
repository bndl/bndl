# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import signal
import threading
import time

from cytoolz.itertoolz import pluck
import psutil

from bndl.compute.tests import DatasetTest
import itertools
from math import ceil
from _collections import OrderedDict


logger = logging.getLogger(__name__)


class ShuffleTest(DatasetTest):
    worker_count = max(3, min(8, ceil(os.cpu_count() * 1.5)))

    config = {
        'bndl.compute.memory.limit': \
            round((200 * 1024 * 1024) / psutil.virtual_memory().total * 100, 2)
    }

    def _test_shuffle(self, **opts):
        data = self.ctx.range(self.size, pcount=self.worker_count * 2).map(str)


        shuffled = data.shuffle(**opts) \
                       .shuffle(**opts, pcount=3, partitioner=lambda i: int(i) % 2) \
                       .collect(parts=True)
        parts = list(filter(None, map(set, shuffled)))

        self.assertEqual(len(parts), 2)
        self.assertEqual(len(parts[0]), self.size / 2)
        self.assertEqual(len(parts[1]), self.size / 2)

        parts.sort(key=lambda part: min(part))
        self.assertEqual(parts[0], self.part0_data)
        self.assertEqual(parts[1], self.part1_data)


sorts = [True, False]
serializations = ['marshal', 'pickle', 'json']
compressions = [None, 'gzip', 'lz4']

options = itertools.product(sorts, serializations, compressions)
for sort, serialization, compression in options:
    opts = OrderedDict()
    opts['sort'] = sort
    opts['serialization'] = serialization
    opts['compression'] = compression

    setattr(
        ShuffleTest,
        'test_shuffle_' + '_'.join(map(lambda o: str(o).lower(), opts.values())),
        lambda self, opts=opts: self._test_shuffle(**opts)
    )

    ShuffleTest.size = 2 * 1000 * 1000
    ShuffleTest.part0_data = set(map(str, range(0, ShuffleTest.size, 2)))
    ShuffleTest.part1_data = set(map(str, range(1, ShuffleTest.size, 2)))



class ShuffleFailureTest(DatasetTest):
    worker_count = 20

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
                        logger.info('Killing %r', self.worker.name)
                        pid = self.worker.service('tasks').execute(lambda: os.getpid()).result()
                        os.kill(pid, signal.SIGKILL)

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
            [100, 30, 30, [125, 125, 125]],
            [100, 30, 30, [75, 100, 125]],
            [100, 30, 30, [100, 100, 100]],
            [100, 30, 30, [0, 50, 100]],
            [ 10, 3, 3, [10]],
            [ 10, 3, 3, [0]],
        ]

        try:
            for dset_size, pcount, key_count, kill_after in test_cases:
                self.ctx.conf['bndl.execute.attempts'] = 2

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
        finally:
            self.ctx.conf['bndl.execute.attempts'] = 1
