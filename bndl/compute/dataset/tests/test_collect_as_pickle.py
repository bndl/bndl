from itertools import chain
from tempfile import TemporaryDirectory
import pickle

from bndl.compute.dataset.tests import DatasetTest
from bndl.util.fs import listdirabs
from bndl.rmi.invocation import InvocationException


class CollectAsPickleTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(1000)

    def test_save(self):
        dsets = (
            self.dset,
            self.dset.map(str),
            self.dset.map(lambda i: dict(key=i)),
        )

        for dset in dsets:
            with TemporaryDirectory() as d:
                dset.collect_as_pickles(d)
                fnames = sorted(listdirabs(d))
                pickles = (pickle.load(open(fname, 'rb')) for fname in fnames)
                elements = list(chain.from_iterable(pickles))
            self.assertEqual(len(fnames), len(dset.parts()))
            self.assertEqual(dset.collect(), elements)


    def test_unpickleable(self):
        with TemporaryDirectory() as d:
            with self.assertRaises(InvocationException):
                self.dset.map(lambda i: lambda i: i).collect_as_pickles(d)
