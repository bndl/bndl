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
import logging

from bndl.net.connection import urlcheck
from bndl.util.aio import run_coroutine_threadsafe


logger = logging.getLogger(__name__)


argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('--listen-addresses', nargs='*', type=urlcheck, dest='listen_addresses',
                       help='The host:port pairs to bind on.')
argparser.add_argument('--seeds', nargs='*', type=urlcheck,
                       help='The host:port pairs at which to "rendevous" with other nodes.')



def start_nodes(nodes):
    started = []
    starts = [run_coroutine_threadsafe(node.start(), node.loop) for node in nodes]
    try:
        for start in starts:
            start.result()
    except Exception:
        logger.exception('Unable to start node')
        stop_nodes(started)
        raise


def stop_nodes(nodes):
    stops = [run_coroutine_threadsafe(node.stop(), node.loop) for node in nodes]
    for stop in stops:
        try:
            stop.result()
        except Exception:
            logger.exception('Unable to stop node')
