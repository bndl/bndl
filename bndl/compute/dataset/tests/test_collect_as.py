from itertools import chain
from tempfile import TemporaryDirectory
import pickle

from bndl.compute.dataset.tests import DatasetTest
from bndl.util.fs import listdirabs
from bndl.rmi.invocation import InvocationException
import json


class CollectAsTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.collection(list(range(1000)))
        self.dsets = (
            self.dset,
            self.dset.map(str),
            self.dset.map(lambda i: dict(key=i)),
            self.dset.glom(),
        )
        
    def test_collect_as_pickles(self):
        for dset in self.dsets:
            with TemporaryDirectory() as d:
                dset.collect_as_pickles(d)
                fnames = sorted(listdirabs(d))
                pickles = (pickle.load(open(fname, 'rb')) for fname in fnames)
                elements = list(chain.from_iterable(pickles))
            self.assertEqual(len(fnames), len(dset.parts()))
            self.assertEqual(dset.collect(), elements)


    def test_collect_as_json(self):
        for dset in self.dsets:
            with TemporaryDirectory() as d:
                dset.collect_as_json(d)
                fnames = sorted(listdirabs(d))
                elements = [
                    json.loads(line)
                    for fname in fnames
                    for line in open(fname, 'r')
                ]
            self.assertEqual(len(fnames), len(dset.parts()))
            self.assertEqual(dset.collect(), elements)


    def test_unpickleable(self):
        with TemporaryDirectory() as d:
            with self.assertRaises(InvocationException):
                self.ctx.range(10, pcount=1).map(lambda i: lambda i: i).collect_as_pickles(d)
