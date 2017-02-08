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

from collections import defaultdict

from bndl.net.tests import NetTest
from bndl.rmi.node import RMINode
import asyncio
from bndl.net.connection import NotConnected


class Service(object):
    def __init__(self, worker):
        self.worker = worker

    @asyncio.coroutine
    def exit(self, src):
        for peer in self.worker.peers.values():
            yield from peer.disconnect('', active=False)
        yield from self.worker.stop()


class DisconnectingNode(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = defaultdict(lambda: 0)
        self.services['test'] = Service(self)

    def call_other(self):
        peer = next(iter(self.peers.values()))
        peer.service('test').exit().result()


class DisconnectedTest(NetTest):
    node_class = DisconnectingNode
    node_count = 2

    def test_single_call(self):
        with self.assertRaises(NotConnected):
            self.nodes[0].call_other()
