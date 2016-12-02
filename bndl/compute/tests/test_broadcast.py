import numpy as np
import pickle
import string

from bndl.compute.tests import DatasetTest
from bndl.compute import broadcast


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
        self.ctx.node._blocks_cache.clear()
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])

    def test_missing(self):
        one = self.ctx.broadcast(1)
        # would normally use unpersist, but this will trip up the worker
        # just to make sure workers don't invent broadcast values out of thin air or something ...
        self.ctx.node._blocks_cache.clear()
        broadcast.download_coordinator.clear(one.block_spec.name)
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
        self.assertEqual(self.ctx.range(len(lst)).map(lambda i: bc_list.value[i]).collect(), lst)

    def test_multiblock(self):
        self.ctx.conf['bndl.compute.broadcast.block_size'] = .5  # 100kb
        # test if threads at a worker play nicely
        self.ctx.conf['bndl.execute.concurrency'] = 8
        lst = list(range(1024 * 1024))  # 10+ blocks
        pickled = pickle.dumps(lst)

        bc_list = self.ctx.broadcast(pickled, None, pickle.loads)
        pcount = self.ctx.worker_count * 16
        dset = self.ctx.range(pcount).map(lambda _: bc_list.value)
        for e in dset.icollect():
            self.assertEqual(e, lst)
