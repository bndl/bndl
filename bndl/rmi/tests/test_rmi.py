from collections import defaultdict

from bndl.net.tests import NetTest
from bndl.rmi.node import RMINode
from bndl.rmi import InvocationException


class NestedRequestNode(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = defaultdict(lambda: 0)

    def call_a(self):
        peer = next(iter(self.peers.values()))
        peer.method_a().result()

    def call_b(self):
        peer = next(iter(self.peers.values()))
        peer.method_b().result()

    def method_a(self, src):
        self.calls['a'] += 1

    def method_b(self, src):
        self.calls['b'] += 1
        peer = next(iter(self.peers.values()))
        peer.method_a().result()

    def method_that_raises(self, src):
        raise ValueError('x')


class RMITest(NetTest):
    node_class = NestedRequestNode
    node_count = 2

    def test_single_call(self):
        self.nodes[0].call_a()
        for node in self.nodes:
            self.assertEqual(len(node.peers), self.node_count - 1)
            self.assertEqual(node.calls['b'], 0)
        self.assertEqual(self.nodes[0].calls['a'], 0)
        self.assertEqual(self.nodes[1].calls['a'], 1)

    def test_nested_requests(self):
        self.nodes[0].call_b()
        for node in self.nodes:
            self.assertEqual(len(node.peers), self.node_count - 1)
        self.assertEqual(self.nodes[0].calls['a'], 1)
        self.assertEqual(self.nodes[0].calls['b'], 0)
        self.assertEqual(self.nodes[1].calls['a'], 0)
        self.assertEqual(self.nodes[1].calls['b'], 1)

    def test_exc(self):
        with self.assertRaises(InvocationException):
            next(iter(self.nodes[0].peers.values())).method_that_raises().result()
