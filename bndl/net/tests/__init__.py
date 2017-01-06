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
from concurrent.futures import Future
from threading import Thread
from unittest.case import TestCase
import asyncio
import logging.config
import sys
import traceback

from bndl.net.node import Node
from bndl.util.aio import get_loop
from bndl.util.exceptions import catch


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
        self._started = Future()
        self._stopped = Future()
        self._loop_thread = Thread(target=self._run_nodes)
        self._loop_thread.start()
        self._started.result()

    def tearDown(self):
        self._stopped.set_result(True)
        self._loop_thread.join()


    def create_nodes(self):
        return [
            self.node_class(addresses=[self.address], seeds=self.seeds, loop=self.loop)
            for _ in range(self.node_count)
        ]

    def _run_nodes(self):
        try:
            self.loop = get_loop(None)
            self.loop.set_debug(True)

            if not self.seeds:
                self.seeds = [
                    self.address + ':%s' % port
                    for port in range(5000, min(5000 + self.node_count, 5004))
                ]

            self.nodes = self.create_nodes()

            @asyncio.coroutine
            def wait_for_discovery(node, peer_count):
                for _ in range(20):
                    if len(node.peers) >= peer_count and all(peer.conn for peer in node.peers.values()):
                        break
                    yield from sleep(.1)

            try:
                # start the nodes
                for i, node in enumerate(self.nodes):
                    self.loop.run_until_complete(node.start())
                    if self.ease_discovery:
                        self.loop.run_until_complete(wait_for_discovery(node, i))
                        self.loop.run_until_complete(sleep(.1))
                # give the nodes a final shot to connect
                for node in self.nodes:
                    self.loop.run_until_complete(wait_for_discovery(node, self.node_count - 1))
                    self.loop.run_until_complete(sleep(.5))
                # notify started
                self._started.set_result(True)
                # wait for stopped notification
                self.loop.run_until_complete(
                    self.loop.run_in_executor(None, self._stopped.result)
                )
            finally:
                with catch(RuntimeError):
                    for node in self.nodes:
                        self.loop.run_until_complete(node.stop())
                    self.loop.run_until_complete(sleep(.2 * len(self.nodes)))
                self.loop.close()
        finally:
            self._started.set_result(True)
