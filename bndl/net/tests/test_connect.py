import os
from socket import AF_INET, AF_UNIX

from bndl.net.node import Node
from bndl.net.tests import NetTest


class TCPTest(NetTest):
    node_count = 4
    seeds = [
        'tcp://localhost.localdomain:%s' % p
        for p in range(5000, 5000 + node_count)
    ]

    def create_nodes(self):
        return [
            Node(loop=self.loop, addresses=[seed], seeds=self.seeds)
            for seed in self.seeds
        ]

    def test_connectivity(self):
        for node in self.nodes:
            for peer in node.peers.values():
                self.assertEqual(peer.conn.socket_family(), AF_INET)


class UDSTest(NetTest):
    node_count = 4
    seeds = [
        'unix:///tmp/bnld.net.tests.test_connect.UDSTest.%s.%s' % (os.getpid(), p)
        for p in range(node_count)
    ]

    def create_nodes(self):
        return [
            Node(loop=self.loop, addresses=[seed], seeds=self.seeds)
            for seed in self.seeds
        ]

    def test_connectivity(self):
        for node in self.nodes:
            for peer in node.peers.values():
                self.assertEqual(peer.conn.socket_family(), AF_UNIX)


class HybridTest(NetTest):
    node_count = 4

    # all listen on a different TCP address
    addresses = [
        ['tcp://127.0.0.%s:5000' % (n // 2 + 1)]
        for n in range(node_count)
    ]

    # start with UDS addresses as seeds
    # nodes may be tempted to connect on this address
    # but should because they're on different hosts (TCP addresses)
    seeds = [
        'unix:///tmp/bndl.net.tests.test_connect.HybridTest.%s.%s' % (os.getpid(), p)
        for p in range(node_count)
    ]

    # add these UDS addresses to the listen addresses
    for i, seed in enumerate(seeds):
        addresses[i].append(seed)

    # add two of the TCP addresses
    # forcing the last two nodes need to discover each other via the first two nodes
    seeds.extend('tcp://127.0.0.%s:5000' % (n + 1) for n in range(2))

    def create_nodes(self):
        return [
            Node(loop=self.loop, addresses=addresses, seeds=self.seeds)
            for addresses in self.addresses
        ]

    def test_connectivity(self):
        for node in self.nodes:
            self.assertEqual(len(node.peers), self.node_count - 1)
            self.assertTrue(all(peer.conn for peer in node.peers.values()))
            self.assertEqual(sum(1 for peer in node.peers.values() if peer.conn.socket_family() == AF_UNIX), 1)
            self.assertEqual(sum(1 for peer in node.peers.values() if peer.conn.socket_family() == AF_INET), 2)
