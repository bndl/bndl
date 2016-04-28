from socket import AF_INET

from bndl.net.node import Node
from bndl.net.tests import NetTest


class TCPTest(NetTest):
    node_count = 4

    def create_nodes(self):
        return [
            Node(loop=self.loop, addresses=[seed], seeds=self.seeds)
            for seed in self.seeds
        ]

    def test_connectivity(self):
        for node in self.nodes:
            for peer in node.peers.values():
                self.assertEqual(peer.conn.socket_family(), AF_INET)
