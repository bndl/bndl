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

from socket import AF_INET, AF_INET6

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
                self.assertIn(peer.conn.socket_family(), (AF_INET, AF_INET6))
