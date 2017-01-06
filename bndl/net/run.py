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

from bndl.net.connection import urlcheck


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
        starts = [node.start() for node in nodes]
        loop.run_until_complete(asyncio.wait(starts, loop=loop))
        if started_signal:
            started_signal.set_result(True)
    except Exception as e:
        if started_signal:
            started_signal.set_result(e)
            return
        else:
            raise

    try:
        atexit.register(loop.stop)
        if stop_signal:
            loop.run_until_complete(loop.run_in_executor(None, stop_signal.result))
        else:
            loop.run_forever()
    finally:
        stops = [node.stop() for node in nodes]
        loop.run_until_complete(asyncio.wait(stops, loop=loop))
