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

from unittest.case import TestCase
import os
import signal
import time

from bndl.compute.worker import start_worker
from bndl.util.aio import run_coroutine_threadsafe
import bndl.compute.worker


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def wait_terminated(pid, timeout=10):
    loops = 100
    for _ in range(loops):
        if not check_pid(pid):
            return True
        else:
            time.sleep(timeout / loops)
    return False


MIN_RUN_TIME = .1

class TestSupervise(TestCase):
    n_executors = 3

    def setUp(self):
        bndl.compute.worker.MIN_RUN_TIME = MIN_RUN_TIME
        self.worker = start_worker(n_executors=self.n_executors)
        self._assert_none_terminated()


    def tearDown(self):
        run_coroutine_threadsafe(self.worker.stop(), self.worker.loop).result(1)


    def _signal_all(self, sig):
        for mon in self.worker._monitors:
            try:
                os.kill(mon.proc.pid, sig)
            except ProcessLookupError:
                pass


    def _assert_all_terminated(self):
        pids = [mon.proc.pid for mon in self.worker._monitors]
        for pid in pids:
            self.assertTrue(wait_terminated(pid), 'pid %s has not terminated' % pid)


    def _assert_none_terminated(self):
        alive = sum(1 for mon in self.worker._monitors if check_pid(mon.proc.pid))
        self.assertEqual(self.n_executors, alive)


    def _terminate_all(self, sig):
        self._signal_all(sig)
        self._assert_all_terminated()


    def _test_stay_dead(self, sig):
        self._terminate_all(sig)
        self._assert_all_terminated()


    def test_stay_dead(self):
        for sig in (signal.SIGKILL, signal.SIGINT):
            self._test_stay_dead(sig)


    def _test_revive(self, sig):
        time.sleep(MIN_RUN_TIME * 2)
        self._terminate_all(sig)

        time.sleep(MIN_RUN_TIME * 2)
        self._assert_none_terminated()


    def test_revive(self):
        for sig in (signal.SIGABRT, signal.SIGBUS, signal.SIGFPE, signal.SIGILL, signal.SIGSEGV):
            self._test_revive(sig)
