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

import time

from bndl.net import watchdog
from bndl.net.tests import NetTest


class ReconnectTestBase(NetTest):
    def all_connected(self):
        con_count = sum(1
                        for node in self.nodes
                        for peer in node.peers.values())
        n = len(self.nodes)
        if con_count != n * (n - 1):
            return False

        for node in self.nodes:
            for peer in node.peers.values():
                if not peer.is_connected:
                    return False
        return True

    def wait_connected(self):
        for _ in range(50):
            time.sleep(watchdog.WATCHDOG_INTERVAL)
            if self.all_connected():
                break


class ReconnectTest(ReconnectTestBase):
    node_count = 4

    def setUp(self):
        super().setUp()
        self.wdog_interval = watchdog.WATCHDOG_INTERVAL
        watchdog.WATCHDOG_INTERVAL = .1


    def tearDown(self):
        super().tearDown()
        watchdog.WATCHDOG_INTERVAL = self.wdog_interval


    def test_disconnect_peer(self):
        self.assertTrue(self.all_connected())

        node = self.nodes[0]
        peer = next(iter(node.peers.values()))
        self.run_coro(peer.disconnect(reason='unit-test'))

        self.wait_connected()
        self.assertTrue(self.all_connected())


    def test_disconnect_node(self):
        self.assertTrue(self.all_connected())

        node = self.nodes[1]
        for peer in node.peers.values():
            self.run_coro(peer.disconnect(reason='unit-test', active=False))

        self.wait_connected()
        self.assertTrue(self.all_connected())

