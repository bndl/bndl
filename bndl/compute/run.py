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

from subprocess import TimeoutExpired
import atexit
import logging
import os

from bndl.compute.context import ComputeContext
from bndl.compute.driver import Driver
from bndl.compute.worker import WorkerSupervisor
from bndl.net.run import start_nodes, stop_nodes
from bndl.util.exceptions import catch
import bndl


def create_ctx(config=None):
    from bndl.util import dash

    if config is None:
        config = bndl.conf

    listen_addresses = config.get('bndl.net.listen_addresses')
    seeds = config.get('bndl.net.seeds')
    worker_count = config.get('bndl.compute.worker_count')

    supervisor = None

    def stop():
        # signal the aio loop can stop and everything can be torn down
        if supervisor:
            supervisor.stop()
            try:
                supervisor.wait()
            except TimeoutExpired:
                pass

        stop_nodes([driver])
        dash.stop()

    driver = Driver(addresses=listen_addresses, seeds=seeds)
    start_nodes([driver])

    try:
        if not seeds and worker_count is None:
            worker_count = os.cpu_count()

        if worker_count:
            args = ['--seeds'] + list((seeds or driver.addresses))
            if listen_addresses:
                args += ['--listen-addresses'] + listen_addresses
            supervisor = WorkerSupervisor(args, worker_count)
            supervisor.start()

        ctx = ComputeContext(driver, config=config)
        dash.run(driver, ctx)
        ctx.add_listener(lambda obj: stop() if obj is ctx and ctx.stopped else None)
        atexit.register(stop)
        atexit.register(ctx.stop)
        return ctx
    except Exception:
        with catch(log_level=logging.WARNING):
            stop()
        raise
