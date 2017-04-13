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

import pickle
import string

from bndl.compute.tests import DatasetTest
import bndl
import numpy as np


class BroadcastTest(DatasetTest):
    serializations = ['auto', 'pickle', 'marshal', 'json', 'text', 'binary']


    def test_broadcast_python_types(self):
        for serialization in self.serializations[:-2]:
            one = self.ctx.broadcast(1, serialization)
            dset = self.ctx.range(1).map(lambda i: one.value)
            self.assertEqual(dset.collect(), [1])

            lst = self.ctx.broadcast([1, 2, 3, 4, 5], serialization)
            dset = self.ctx.range(1).map(lambda i: lst.value)
            self.assertEqual(dset.collect(), [[1, 2, 3, 4, 5]])


    def test_broadcast_string(self):
        for serialization in self.serializations[:-1]:
            lowercase = self.ctx.broadcast(string.ascii_lowercase, serialization)
            dset = self.ctx.range(1).map(lambda i: lowercase.value)
            self.assertEqual(dset.collect(), [string.ascii_lowercase])


    def test_broadcast_ndarray(self):
        for serialization in self.serializations[:2]:
            arr = self.ctx.broadcast(np.arange(10), serialization)
            dset = self.ctx.range(1).map(lambda i: arr.value)
            self.assertEqual(dset.first().tolist(), np.arange(10).tolist())


    def test_local_access(self):
        lowercase = self.ctx.broadcast(string.ascii_lowercase)
        self.assertEqual(lowercase.value, string.ascii_lowercase)


    def test_cached(self):
        one = self.ctx.broadcast(1)
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])
        # would normally use unpersist, but this will trip up the worker if it wasn't cached
        self.ctx.node.service('blocks').blocks.clear()
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])


    def test_missing(self):
        one = self.ctx.broadcast(1)
        # would normally use unpersist, but this will trip up the worker
        # just to make sure workers don't invent broadcast values out of thin air or something ...
        self.ctx.node.service('blocks').blocks.clear()
        with self.assertRaises(Exception):
            self.ctx.range(1).map(lambda i: one.value).collect()


    def test_unpersist(self):
        one = self.ctx.broadcast(1)
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])

        one.unpersist()

        with self.assertRaises(Exception):
            self.ctx.range(1).map(lambda i: one.value).collect()


    def test_pickled(self):
        lst = list(range(1000 * 1000))
        pickled = pickle.dumps(lst)
        bc_list = self.ctx.broadcast(pickled, None, pickle.loads)
        self.assertEqual(self.ctx.range(1000).map(lambda i: bc_list.value[i]).collect(), lst[:1000])


    def test_multiblock(self):
        bndl.conf['bndl.compute.blocks.min_download_size_mb'] = .100
        bndl.conf['bndl.compute.blocks.max_download_size_mb'] = .100
        bndl.conf['bndl.compute.concurrency'] = 8 # test if threads at a worker play nicely
        lst = list(range(256 * 1024))  # 10+ blocks
        bc_list = self.ctx.broadcast(lst)
        pcount = self.ctx.executor_count * 16
        dset = self.ctx.range(pcount).map(lambda _: bc_list.value)
        for e in dset.icollect():
            self.assertEqual(e, lst)
