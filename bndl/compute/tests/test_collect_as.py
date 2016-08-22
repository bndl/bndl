from io import StringIO
from itertools import chain
import json
import pickle
from tempfile import TemporaryDirectory

from bndl.compute.tests import DatasetTest
from bndl.rmi import InvocationException
from bndl.util.fs import listdirabs, read_file
import gzip


class CollectAsTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.collection(list(range(100)), pcount=3)
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
            self.check_elements(dset, fnames, elements,'.p')


    def test_collect_as_json(self):
        for dset in self.dsets:
            with TemporaryDirectory() as d:
                dset.collect_as_json(d)
                fnames = sorted(listdirabs(d))
                data = b''.join(read_file(fname) for fname in fnames).decode()
                elements = [json.loads(line) for line in StringIO(data)]
            self.check_elements(dset, fnames, elements,'.json')


    def test_collect_gzipped(self):
        with TemporaryDirectory() as d:
            self.dset.collect_as_json(d, compress='gzip')
            fnames = sorted(listdirabs(d))
            elements = [
                json.loads(line.decode())
                for fname in fnames
                for line in gzip.open(fname)
            ]
            self.check_elements(self.dset, fnames, elements, '.json.gz')


    def check_elements(self, dset, fnames, elements, extension):
        self.assertEqual(len(fnames), len(dset.parts()))
        self.assertEqual(dset.collect(), elements)
        for fname in fnames:
            self.assertTrue(fname.endswith(extension))


    def test_unpickleable(self):
        with TemporaryDirectory() as d:
            with self.assertRaises(InvocationException):
                self.ctx.range(10, pcount=1).map(lambda i: lambda i: i).collect_as_pickles(d)
