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

from concurrent.futures._base import TimeoutError
import sys
import unittest

from bndl.compute.run import create_ctx
from bndl.compute.worker import start_worker
from bndl.util.aio import run_coroutine_threadsafe
from bndl.util.conf import Config
import bndl


class ComputeTest(unittest.TestCase):
    executor_count = 3
    config = {}

    @classmethod
    def setUpClass(cls):
        # Increase switching interval to lure out race conditions a bit ...
        sys.setswitchinterval(1e-6)

        bndl.conf['bndl.compute.executor_count'] = 0
        bndl.conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.1:0'
        bndl.conf.update(cls.config)

        cls.ctx = create_ctx()

        cls.workers = []
        if cls.executor_count > 0:
            bndl.conf['bndl.net.seeds'] = cls.ctx.node.addresses
            n_workers = cls.executor_count // 2
            for i in range(n_workers):
                bndl.conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.%s:0' % (i + 1)
                worker = start_worker()
                cls.workers.append(worker)

            for i in range(cls.executor_count):
                worker = cls.workers[i % n_workers]
                worker.start_executors(1)

        for _ in range(2):
            cls.ctx.await_executors(cls.executor_count, 120, 120)
        assert cls.ctx.executor_count == cls.executor_count, \
            '%s != %s' % (cls.ctx.executor_count, cls.executor_count)

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()
        try:
            for w in cls.workers:
                w.stop_async().result(60)
        except TimeoutError:
            pass
        bndl.conf.clear()
        sys.setswitchinterval(5e-3)


class DatasetTest(ComputeTest):
    pass
