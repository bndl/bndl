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
