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

from concurrent.futures import TimeoutError
from urllib.parse import urlunparse
import atexit
import logging
import os
import re
import signal
import sys

from bndl.compute.blocks import BlockManager
from bndl.compute.broadcast import BroadcastManager
from bndl.compute.memory import LocalMemoryManager
from bndl.compute.shuffle import ShuffleManager
from bndl.compute.tasks import Tasks
from bndl.net.aio import get_loop, get_loop_thread, stop_loop, run_coroutine_threadsafe
from bndl.net.connection import urlparse
from bndl.net.rmi import RMINode
from bndl.util.funcs import noop
from bndl.util.threads import dump_threads
import resource


logger = logging.getLogger(__name__)


class Executor(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.memory_manager = LocalMemoryManager()
        self.services['blocks'] = BlockManager(self)
        self.services['broadcast'] = BroadcastManager(self)
        self.services['shuffle'] = ShuffleManager(self)
        self.services['tasks'] = Tasks(self)
        self.services['memory'] = self.memory_manager


def _executor_address(worker_address, executor_id):
    worker_address = urlparse(worker_address)
    executor_address = list(worker_address)
    executor_port = worker_address.port + int(executor_id)
    executor_address[1] = re.sub(':\d+', ':%s' % (executor_port), executor_address[1])
    executor_address = urlunparse(executor_address)
    return executor_address


def main():
    # Make sure numpy, scipy etc don't ask OMP to create as much threads as there are cores as BNDL is
    # already parallelizing work.
    if 'OMP_NUM_THREADS' not in os.environ:
        os.environ['OMP_NUM_THREADS'] = '2'

    # Set the soft limit for the maximum number of open files to the hard limit.
    for r in (resource.RLIMIT_NOFILE,):
        low, high = resource.getrlimit(r)
        if low < high or high == resource.RLIM_INFINITY:
            resource.setrlimit(r, (high, high))

    worker_address, executor_id = sys.argv[1:]
    executor_address = _executor_address(worker_address, executor_id)

    loop = get_loop(start=True)
    executor = Executor(
        node_type='executor',
        addresses=[executor_address],
        seeds=[worker_address],
        loop=loop,
    )
    executor.start_async().result()

    @atexit.register
    def stop(*args):
        if executor.running and loop.is_running():
            try:
                run_coroutine_threadsafe(executor.stop(), loop).result(1)
            except TimeoutError:
                pass

        stop_loop()

    def exit_handler(sig, frame):
        stop()

    signal.signal(signal.SIGINT, noop)
    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGUSR1, dump_threads)

    get_loop_thread().join()


if __name__ == '__main__':
    main()
