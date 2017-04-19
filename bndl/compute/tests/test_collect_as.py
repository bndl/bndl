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

from io import StringIO
from itertools import chain
from tempfile import TemporaryDirectory
import gzip
import json
import pickle

from bndl.compute.dataset import Dataset
from bndl.compute.tests import ComputeTest
from bndl.net.rmi import InvocationException
from bndl.util.compat import lz4_decompress
from bndl.util.fs import listdirabs, read_file


class CollectAsTest(ComputeTest):
    def setUp(self):
        self.dset = self.ctx.collection(list(range(100)), pcount=3)
        self.dsets = (
            self.dset,
            self.dset.map(str),
            self.dset.map(lambda i: dict(key=i)),
            self.dset.glom(),
        )


    def test_collect_as_pickles(self):
        for save in (Dataset.collect_as_pickles, Dataset.save_as_pickles):
            for dset in self.dsets:
                with TemporaryDirectory() as d:
                    save(dset, d)
                    fnames = sorted(listdirabs(d))
                    pickles = (pickle.load(open(fname, 'rb')) for fname in fnames)
                    elements = list(chain.from_iterable(pickles))
                self.check_elements(dset, fnames, elements, '.p')


    def test_collect_as_json(self):
        for save in (Dataset.collect_as_json, Dataset.save_as_json):
            for dset in self.dsets:
                with TemporaryDirectory() as d:
                    save(dset, d)
                    fnames = sorted(listdirabs(d))
                    data = b''.join(read_file(fname) for fname in fnames).decode()
                    elements = [json.loads(line) for line in StringIO(data)]
                self.check_elements(dset, fnames, elements, '.json')


    def test_collect_compressed(self):
        for save in (Dataset.collect_as_json, Dataset.save_as_json):
            compressions = (
                ('gzip', '.gz', gzip.decompress),
                ('lz4', '.lz4', lz4_decompress)
            )
            for compression, ext, decompress in compressions:
                with TemporaryDirectory() as d:
                    save(self.dset, d, compression=compression)
                    fnames = sorted(listdirabs(d))
                    elements = [
                        json.loads(line)
                        for fname in fnames
                        for line in decompress(open(fname, 'rb').read()).decode().splitlines()
                    ]
                    self.check_elements(self.dset, fnames, elements, '.json' + ext)


    def check_elements(self, dset, fnames, elements, extension):
        self.assertEqual(len(fnames), len(dset.parts()))
        self.assertEqual(dset.collect(), elements)
        for fname in fnames:
            self.assertTrue(fname.endswith(extension))


    def test_unpickleable(self):
        with TemporaryDirectory() as d:
            with self.assertRaises(InvocationException):
                self.ctx.range(10, pcount=1).map(lambda i: lambda i: i).collect_as_pickles(d)
