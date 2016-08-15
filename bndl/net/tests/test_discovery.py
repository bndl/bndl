from bndl.net.tests import NetTest


class DiscoveryTestMixin:
    def test_discovery(self):
        for node in self.nodes:
            for peer in node.peers.values():
                self.assertTrue(peer.conn)
            self.assertEqual(len(node.peers), self.node_count - 1)


class DiscoveryTest(NetTest, DiscoveryTestMixin):
    node_count = 4
    
    def test_discovery(self):
        super().test_discovery()


class JustTwoTest(NetTest, DiscoveryTestMixin):
    node_count = 2

    def test_discovery(self):
        super().test_discovery()
        self.assertEqual(list(self.nodes[0].peers), [self.nodes[1].name])
        self.assertEqual(list(self.nodes[1].peers), [self.nodes[0].name])


class AllAtOnceTest(DiscoveryTest):
    ease_discovery = False


class ManyNodesTest(DiscoveryTest):
    node_count = 8
