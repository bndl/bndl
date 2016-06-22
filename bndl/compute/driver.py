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
argparser.add_argument('--workers', nargs='?', type=int, default=0, dest='worker_count')
argparser.add_argument('--conf', nargs='*', default=())
argparser.set_defaults(seeds=())


def run_bndl(args, conf, started, stopped):
    if not args.seeds and not args.worker_count:
        args.worker_count = os.cpu_count()
        args.seeds = args.listen_addresses or ['tcp://%s:5000' % socket.getfqdn()]

    driver = create_node(Driver, args)
    loop = driver.loop

    supervisor = Supervisor(loop, 'bndl.compute.worker', 'main', ['--seeds'] + driver.addresses, args.worker_count)

    global ctx
    ctx = ComputeContext(driver, conf)

    dash.run(driver, ctx)

    @asyncio.coroutine
    def wait_for_peers():
        global ctx
        worker_count = max(args.worker_count, 1)
        timeout = int(1 + math.log2(worker_count) * 10)
        for _ in range(timeout):
            if ctx.worker_count >= worker_count:
                break
            yield from asyncio.sleep(.2)  # @UndefinedVariable
        if ctx.worker_count < worker_count:
            raise Exception("can't bootstrap workers")

    try:
        # start driver, supervisor and wait for them to connect
        try:
            loop.run_until_complete(driver.start())
            loop.run_until_complete(supervisor.start())
            loop.run_until_complete(wait_for_peers())
        except:
            started.set_result(True)
            return

        # signal the set up is done
        started.set_result(True)
        # run the aio loop until the stop signal is given
        until_stopped = loop.run_in_executor(None, stopped.result)
        loop.run_until_complete(until_stopped)
    finally:
        loop.run_until_complete(driver.stop())
        loop.run_until_complete(supervisor.stop())

        # close the loop
        loop.close()



def main(args=None, daemon=True):
    if isinstance(args, argparse.Namespace):
        args = args
    else:
        args = argparser.parse_args(args)

    conf = dict(c.split('=', 1) for c in args.conf)

    # signals for starting and stopping
    started = concurrent.futures.Future()
    stopped = concurrent.futures.Future()

    # start the thread to set up the driver etc. and run the aio loop
    bndl_thread = threading.Thread(target=run_bndl, args=(args, conf, started, stopped), daemon=daemon)
    bndl_thread.start()
    # wait for driver, workers etc. to set up
    started.result()

    def stop():
        # signal the aio loop can stop and everything can be torn down
        if not stopped.done():
            stopped.set_result(True)
            # wait for everything to stop
            bndl_thread.join(timeout=5)

    ctx.add_listener(lambda ctx: stop() if isinstance(ctx, ComputeContext) else None)
    atexit.register(stop)

    return ctx
