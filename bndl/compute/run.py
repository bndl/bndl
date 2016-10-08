from subprocess import TimeoutExpired
import atexit
import concurrent.futures
import os
import threading

from bndl.util import dash
from bndl.compute.context import ComputeContext
from bndl.compute.driver import Driver
from bndl.net.run import run_nodes
from bndl.util.aio import get_loop
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.objects import LazyObject
from bndl.util.supervisor import Supervisor


def create_ctx(config=Config(), daemon=True):
    listen_addresses = config.get('bndl.net.listen_addresses')
    seeds = config.get('bndl.net.seeds')
    worker_count = config.get('bndl.compute.worker_count')

    if not seeds and worker_count is None:
        worker_count = os.cpu_count()

    # signals for starting and stopping
    started = concurrent.futures.Future()
    stopped = concurrent.futures.Future()

    # start the thread to set up the driver and run the aio loop
    loop = get_loop()
    driver = Driver(addresses=listen_addresses, seeds=seeds, loop=loop)
    driver_thread = threading.Thread(target=run_nodes, daemon=daemon, args=(driver,),
                                     kwargs=dict(started_signal=started, stop_signal=stopped),
                                     name='bndl-driver-thread')
    driver_thread.start()

    # start the supervisor
    supervisor = None

    def stop():
        # signal the aio loop can stop and everything can be torn down
        if not stopped.done():
            if supervisor:
                supervisor.stop()
                try:
                    supervisor.wait(timeout=5)
                except TimeoutExpired:
                    pass
            stopped.set_result(True)
            driver_thread.join(timeout=5)
            dash.stop()

    try:
        # wait for driver to set up
        result = started.result()
        if isinstance(result, Exception):
            raise result

        if worker_count:
            args = ['--seeds'] + list((seeds or driver.addresses))
            if listen_addresses:
                args += ['--listen-addresses'] + listen_addresses
            supervisor = Supervisor('bndl.compute.worker', 'main', args, worker_count)
            supervisor.start()

        # create compute context
        ctx = ComputeContext(driver, config=config)

        # start dash board
        dash.run(driver, ctx)

        # register stop as 'exit' listeners
        ctx.add_listener(lambda obj: stop() if obj is ctx else None)
        atexit.register(stop)
        atexit.register(ctx.stop)
        return ctx
    except Exception:
        with catch():
            stop()
        raise


def _get_or_create_ctx():
    if len(ComputeContext.instances) > 0:
        return next(iter(ComputeContext.instances))
    else:
        return create_ctx()

ctx = LazyObject(_get_or_create_ctx, 'stop')
