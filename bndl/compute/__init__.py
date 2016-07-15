from subprocess import TimeoutExpired
import argparse
import atexit
import copy
import os
import socket
import threading

import concurrent.futures

from bndl import dash, net
from bndl.compute import driver
from bndl.compute.context import ComputeContext
from bndl.compute.driver import Driver
from bndl.net.connection import urlparse
from bndl.net.run import run_nodes
from bndl.util import conf
from bndl.util.aio import get_loop
from bndl.util.conf import Config
from bndl.util.objects import LazyObject
from bndl.util.supervisor import Supervisor


worker_count = conf.Int()



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
                                     kwargs=dict(started_signal=started, stop_signal=stopped))
    driver_thread.start()

    # start the supervisor
    supervisor = None

    def stop():
        # signal the aio loop can stop and everything can be torn down
        if not stopped.done():
            try:
                if supervisor:
                    supervisor.stop()
                stopped.set_result(True)
                driver_thread.join()
                dash.stop()
            except TimeoutExpired:
                pass

    try:
        # wait for driver to set up
        result = started.result()
        if isinstance(result, Exception):
            raise result

        if worker_count:
            supervisor = Supervisor('bndl.compute.worker', 'main', ['--seeds'] + (seeds or driver.addresses), worker_count)
            supervisor.start()

        # create compute context
        ctx = ComputeContext(driver, config=config)

        # start dash board
        dash.run(driver, ctx)

        # register stop as 'exit' listeners
        ctx.add_listener(lambda obj: stop() if ctx is obj else None)
        atexit.register(stop)
    except Exception:
        stop()

    return ctx


ctx = LazyObject(lambda: create_ctx(daemon=True), 'stop')
