import argparse
import asyncio
import atexit
import copy
import logging
import math
import os
import socket
import threading

import concurrent.futures

from bndl import dash
from bndl.compute.context import ComputeContext
from bndl.compute.worker import Worker, argparser
from bndl.net.run import create_node
from bndl.util.supervisor import Supervisor
from subprocess import TimeoutExpired


logger = logging.getLogger(__name__)


class Driver(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broadcast_values = {}


    def get_broadcast_value(self, src, key):
        logger.debug('sending broadcast value with key %s to %s', key, src)
        return self.broadcast_values[key]


    def unpersist_broadcast_value(self, key):
        logger.debug('removing broadcast value with key %s to', key)
        if key in self.broadcast_values:
            del self.broadcast_values[key]
        # TODO better error handling
        futures = []
        for worker in self.peers.filter(node_type='worker'):
            try:
                futures.append(worker.unpersist_broadcast_value(key))
            except:
                pass
        # wait for all to be done
        [future.result() for future in futures]



argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.driver'
argparser.add_argument('--workers', nargs='?', type=int, default=None, dest='worker_count')
argparser.add_argument('--conf', nargs='*', default=())
argparser.set_defaults(seeds=())


def run_driver(args, conf, started, stopped):
    if not args.seeds:
        args.seeds = args.listen_addresses or ['tcp://%s:5000' % socket.getfqdn()]

    driver = create_node(Driver, args)
    loop = driver.loop

    ctx = ComputeContext(driver, conf)

    dash.run(driver, ctx)

    try:
        try:
            loop.run_until_complete(driver.start())
        except:
            started.set_result(True)
            return

        # signal the set up is done and run the aio loop until the stop signal is given
        started.set_result((driver, ctx))
        loop.run_until_complete(loop.run_in_executor(None, stopped.result))
    finally:
        # stop the driver and close the loop
        loop.run_until_complete(driver.stop())
        loop.close()


def main(args=None, daemon=True):
    if isinstance(args, argparse.Namespace):
        args = args
    else:
        args = argparser.parse_args(args)

    if not args.seeds and args.worker_count is None:
        args.worker_count = os.cpu_count()

    conf = dict(c.split('=', 1) for c in args.conf)

    # signals for starting and stopping
    started = concurrent.futures.Future()
    stopped = concurrent.futures.Future()

    # start the thread to set up the driver etc. and run the aio loop
    driver_thread = threading.Thread(target=run_driver, args=(args, conf, started, stopped), daemon=daemon)
    driver_thread.start()

    # wait for driver to set up
    result = started.result()
    if not result:
        return None
    else:
        driver, ctx = result

    # start the supervisor
    supervisor = None
    if args.worker_count:
        supervisor = Supervisor('bndl.compute.worker', 'main', ['--seeds'] + driver.addresses, args.worker_count)
        supervisor.start()

    def stop():
        # signal the aio loop can stop and everything can be torn down
        if not stopped.done():
            try:
                if supervisor:
                    supervisor.stop()
                    supervisor.wait(timeout=1)
                stopped.set_result(True)
                driver_thread.join(timeout=1)
            except TimeoutExpired:
                pass

    ctx.add_listener(lambda ctx: stop() if isinstance(ctx, ComputeContext) else None)
    atexit.register(stop)

    try:
        ctx.await_workers()
    except:
        pass

    return ctx
