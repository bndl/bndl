import string

from bndl.compute.dataset.tests import DatasetTest
import numpy as np


class BroadcastTest(DatasetTest):

    def test_broadcast_scalar(self):
        one = self.ctx.broadcast(1)
        dset = self.ctx.range(1).map(lambda i: one.value)
        self.assertEqual(dset.collect(), [1])

    def test_broadcast_list(self):
        lst = self.ctx.broadcast([1, 2, 3, 4, 5])
        dset = self.ctx.range(1).map(lambda i: lst.value)
        self.assertEqual(dset.collect(), [[1, 2, 3, 4, 5]])

    def test_broadcast_string(self):
        lowercase = self.ctx.broadcast(string.ascii_lowercase)
        dset = self.ctx.range(1).map(lambda i: lowercase.value)
        self.assertEqual(dset.collect(), [string.ascii_lowercase])

    def test_broadcast_ndarray(self):
        arr = self.ctx.broadcast(np.arange(10))
        dset = self.ctx.range(1).map(lambda i: arr.value)
        self.assertEqual(dset.first().tolist(), np.arange(10).tolist())

    def test_local_access(self):
        lowercase = self.ctx.broadcast(string.ascii_lowercase)
        self.assertEqual(lowercase.value, string.ascii_lowercase)

    def test_cached(self):
        one = self.ctx.broadcast(1)
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])
        # would normally use unpersist, but this will trip up the worker if it wasn't cached
        self.ctx.node.broadcast_values.clear()
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])

    def test_missing(self):
        one = self.ctx.broadcast(1)
        # would normally use unpersist, but this will trip up the worker
        # just to make sure workers don't invent broadcast values out of thin air or something ...
        self.ctx.node.broadcast_values.clear()
        with self.assertRaises(Exception):
            self.ctx.range(1).map(lambda i: one.value).collect()

    def test_unpersist(self):
        one = self.ctx.broadcast(1)
        self.assertEqual(self.ctx.range(1).map(lambda i: one.value).collect(), [1])

        one.unpersist()

        with self.assertRaises(Exception):
            self.ctx.range(1).map(lambda i: one.value).collect()
