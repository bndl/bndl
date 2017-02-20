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

import sys
import unittest

from bndl.compute.run import create_ctx
from bndl.compute.worker import run_workers, WorkerSupervisor
from bndl.util.conf import Config
import bndl


class ComputeTest(unittest.TestCase):
    worker_count = 3
    config = {}

    @classmethod
    def setUpClass(cls):
        # Increase switching interval to lure out race conditions a bit ...
        sys.setswitchinterval(1e-6)

        config = bndl.conf
        config['bndl.compute.worker_count'] = 0
        config['bndl.net.listen_addresses'] = 'tcp://127.0.0.1:0'
        config.update(cls.config)
        cls.ctx = create_ctx(config)

        cls.node_count = 0 if not cls.worker_count else cls.worker_count // 2 + 1
        cls.supervisors = []
        for i in range(cls.worker_count):
            args = ('--listen-addresses', 'tcp://127.0.0.%s:0' % (i // 2 + 1),
                    '--seeds', cls.ctx.node.addresses[0])
            superv = WorkerSupervisor(args, process_count=1)
            superv.start()
            cls.supervisors.append(superv)

        cls.ctx.await_workers(cls.worker_count, 120, 120)
        assert cls.ctx.worker_count == cls.worker_count, \
            '%s != %s' % (cls.ctx.worker_count, cls.worker_count)

    @classmethod
    def tearDownClass(cls):
        sys.setswitchinterval(5e-3)
        cls.ctx.stop()
        for superv in cls.supervisors:
            superv.stop()


class DatasetTest(ComputeTest):
    pass
