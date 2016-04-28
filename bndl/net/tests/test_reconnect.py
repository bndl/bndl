import time

from bndl.net.tests import NetTest
from bndl.net.watchdog import WATCHDOG_INTERVAL
from bndl.util import aio


class ReconnectTest(NetTest):
    node_count = 4


    def all_connected(self):
        for node in self.nodes:
            for peer in node.peers.values():
                if not peer.is_connected:
                    return False
        return True

    def wait_connected(self, node):
        for _ in range(30):
            time.sleep(WATCHDOG_INTERVAL / 10)
            if self.all_connected():
                break

    def test_disconnect(self):
        self.assertTrue(self.all_connected())

        node = self.nodes[0]
        peer = next(iter(node.peers.values()))
        aio.run_coroutine_threadsafe(peer.disconnect(reason='unit-test', active=False), self.loop)

        self.wait_connected(node)
        self.assertTrue(self.all_connected())

        node = self.nodes[1]
        for server in node.servers.values():
            server.close()

        for peer in node.peers.values():
            aio.run_coroutine_threadsafe(peer.disconnect(reason='unit-test', active=False), self.loop)

        self.wait_connected(node)
        self.assertTrue(self.all_connected())

