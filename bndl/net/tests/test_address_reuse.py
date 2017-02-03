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

from bndl.net import watchdog
from bndl.net.tests.test_reconnect import ReconnectTestBase


class AddressReuseTest(ReconnectTestBase):
    node_count = 4

    def test_node_name_change(self):
        wdog_interval = watchdog.WATCHDOG_INTERVAL
        watchdog.WATCHDOG_INTERVAL = .5
        try:
            self.assertTrue(self.all_connected())

            node = self.nodes[0]
            node.name = 'something-else'
            for peer in node.peers.values():
                peer.disconnect_async(reason='unit-test', active=False).result()

            self.wait_connected()
            self.assertTrue(self.all_connected())
        finally:
            watchdog.WATCHDOG_INTERVAL = wdog_interval
