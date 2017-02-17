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

from asyncio import sleep  # @UnresolvedImport
from concurrent.futures import Future, wait
from threading import Thread
from unittest.case import TestCase
import asyncio
import logging.config
import sys
import traceback

from bndl.net.node import Node
from bndl.util.aio import get_loop, run_coroutine_threadsafe
from bndl.util.exceptions import catch
import time
import concurrent


@asyncio.coroutine
def wait_for_discovery(node, peer_count):
    for _ in range(20):
        connected_count = sum(1 for peer in node.peers.values() if peer.is_connected)
        if connected_count >= peer_count:
            break
        yield from sleep(.1, loop=node.loop)


class NetTest(TestCase):
    address = 'tcp://127.0.0.10'
    node_class = Node
    node_count = 4
    ease_discovery = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seeds = []
        self.nodes = []


    def setUp(self):
        self.loop = get_loop()
        self.loop.set_debug(True)

        if not self.seeds:
            self.seeds = [
                self.address + ':%s' % port
                for port in range(5000, min(5000 + self.node_count, 5004))
            ]

        self.nodes = self.create_nodes()

        for i, node in enumerate(self.nodes):
            run_coroutine_threadsafe(node.start(), loop=self.loop).result()
            if self.ease_discovery:
                run_coroutine_threadsafe(wait_for_discovery(node, i), loop=self.loop).result()

        for node in self.nodes:
            run_coroutine_threadsafe(wait_for_discovery(node, self.node_count - 1), loop=self.loop).result()
            # time.sleep(.5)


    def tearDown(self):
        wait([
            run_coroutine_threadsafe(node.stop(), loop=self.loop)
            for node in self.nodes
        ])
        # time.sleep(.2 * len(self.nodes))



    def create_nodes(self):
        return [
            self.node_class(addresses=[self.address], seeds=self.seeds, loop=self.loop)
            for _ in range(self.node_count)
        ]
