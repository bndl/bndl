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
from unittest.loader import TestLoader
import os
import sys
import time
import unittest

from bndl.compute.context import ComputeContext
from bndl.compute.tests.helper import stop_global_test_ctx, global_test_ctx, start_test_ctx, \
    stop_test_ctx, get_global_test_ctx
from bndl.compute.worker import start_worker
from bndl.net.aio import run_coroutine_threadsafe, get_loop
from bndl.util.collection import flatten
from bndl.util.conf import Config
import bndl


class ComputeTest(unittest.TestCase):
    executor_count = 3
    config = {}
    _tear_down_ctx = False

    @classmethod
    def setUpClass(cls):
        # Increase switching interval to lure out race conditions a bit ...
        sys.setswitchinterval(1e-6)
        if cls.executor_count != 3 or cls.config:
            bndl.conf.update(cls.config)
            cls.ctx, cls.workers = start_test_ctx(cls.executor_count)
        else:
            cls.ctx, cls.workers = get_global_test_ctx()
            if cls.ctx is None:
                assert cls.workers is None
                cls.ctx, cls.workers = start_test_ctx(cls.executor_count)
                cls._tear_down_ctx = True

    @classmethod
    def tearDownClass(cls):
        if cls._tear_down_ctx:
            stop_test_ctx(cls.ctx, cls.workers)
        bndl.conf.clear()
        sys.setswitchinterval(5e-3)


class ComputeTestSuite(unittest.TestSuite):
    def run(self, result, debug=False):
        global_test_ctx(3)
        result = unittest.TestSuite.run(self, result, debug=debug)
        stop_global_test_ctx()
        return result


def load_tests(loader, standard_tests, pattern):
    suite = ComputeTestSuite()
    suite.addTests(loader.discover(start_dir=os.path.dirname(__file__),
                                   pattern=pattern or 'test*.py'))
    return suite
