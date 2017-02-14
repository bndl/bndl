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

import argparse
import asyncio
import atexit
import concurrent.futures

from bndl.net.connection import urlcheck
from bndl.util.aio import run_coroutine_threadsafe


argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('--listen-addresses', nargs='*', type=urlcheck, dest='listen_addresses',
                       help='The host:port pairs to bind on.')
argparser.add_argument('--seeds', nargs='*', type=urlcheck,
                       help='The host:port pairs at which to "rendevous" with other nodes.')


def exception_handler(loop, context):
    exc = context.get('exception')
    if type(exc) is not SystemExit:
        loop.default_exception_handler(context)


def run_nodes(*nodes, started_signal=None, stop_signal=None):
    assert len(nodes) > 0

    loop = nodes[0].loop
    assert all(node.loop == loop for node in nodes)
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(exception_handler)

    try:
        futs = [run_coroutine_threadsafe(node.start(), loop)
                for node in nodes]

        if started_signal:
            for fut in futs:
                fut.result()
            started_signal.set_result(True)
    except Exception as e:
        if started_signal:
            started_signal.set_result(e)
            return
        else:
            raise

    try:
        if not stop_signal:
            stop_signal = concurrent.futures.Future()
            atexit.register(stop_signal.set_result, (True,))
        stop_signal.result()
    finally:
        for node in nodes:
            loop.create_task(node.stop())
