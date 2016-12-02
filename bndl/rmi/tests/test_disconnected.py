from collections import defaultdict

from bndl.net.tests import NetTest
from bndl.rmi.node import RMINode
import asyncio
from bndl.net.connection import NotConnected


class DisconnectingNode(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = defaultdict(lambda: 0)

    def call_other(self):
        peer = next(iter(self.peers.values()))
        peer.exit().result()

    @asyncio.coroutine
    def exit(self, src):
        for peer in self.peers.values():
            yield from peer.disconnect('', active=False)
        yield from self.stop()


class DisconnectedTest(NetTest):
    node_class = DisconnectingNode
    node_count = 2

    def test_single_call(self):
        with self.assertRaises(NotConnected):
            self.nodes[0].call_other()
