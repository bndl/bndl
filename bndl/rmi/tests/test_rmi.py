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
from bndl.rmi import InvocationException


class Service(object):
    def __init__(self, worker):
        self.worker = worker

    def method_a(self, src):
        self.worker.calls['a'] += 1

    def method_b(self, src):
        self.worker.calls['b'] += 1
        peer = next(iter(self.worker.peers.values()))
        peer.service('test').method_a().result()

    def method_that_raises(self, src):
        raise ValueError('x')
        

class NestedRequestNode(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.services['test'] = Service(self)
        self.calls = defaultdict(lambda: 0)

    def call_a(self):
        peer = next(iter(self.peers.values()))
        peer.service('test').method_a().result()

    def call_b(self):
        peer = next(iter(self.peers.values()))
        peer.service('test').method_b.with_timeout(5)().result()


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
        peer = next(iter(self.nodes[0].peers.values()))
        with self.assertRaises(InvocationException):
            peer.service('test').method_that_raises().result()
