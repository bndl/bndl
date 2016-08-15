from asyncio import sleep  # @UnresolvedImport
import asyncio
from concurrent.futures import Future
import logging.config
from threading import Thread
import traceback
from unittest.case import TestCase

from bndl.net.node import Node
from bndl.util.aio import get_loop


class NetTest(TestCase):
    node_class = Node
    node_count = 4
    ease_discovery = True
    seeds = []
    nodes = []


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
            self.node_class(seeds=self.seeds, loop=self.loop)
            for _ in range(self.node_count)
        ]

    def _run_nodes(self):
        try:
            self.loop = get_loop(None)
            self.loop.set_debug(True)

            if not self.seeds:
                self.seeds = [
                    'tcp://localhost.localdomain:%s' % port
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
                        self.loop.run_until_complete(sleep(.01))
                # give the nodes a final shot to connect
                for node in self.nodes:
                    self.loop.run_until_complete(wait_for_discovery(node, self.node_count - 1))
                    self.loop.run_until_complete(sleep(.25))
                # notify started
                self._started.set_result(True)
                # wait for stopped notification
                self.loop.run_until_complete(
                    self.loop.run_in_executor(None, self._stopped.result)
                )
            finally:
                for node in self.nodes:
                    self.loop.run_until_complete(node.stop())
                    self.loop.run_until_complete(sleep(.1))

                self.loop.close()
        finally:
            self._started.set_result(True)
