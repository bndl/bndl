from itertools import chain
from tempfile import TemporaryDirectory
import gzip
import os.path

from bndl.compute.dataset.tests import DatasetTest
from bndl.util.text import random_string


class FilesTest(DatasetTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tmpdir = TemporaryDirectory('bndl_unit_test')
        for idx in range(10):
            with open(os.path.join(cls.tmpdir.name, 'decoy_file_%s.tmp' % idx), 'w') as f:
                print('decoy', file=f)
        cls.contents = [('\n'.join(random_string(127) for _ in range(8)) + '\n').encode() for _ in range(32)]
        cls.files = [
            open(os.path.join(cls.tmpdir.name, 'test_file_%s.tmp' % idx), 'wb')
            for idx in range(len(cls.contents))
        ]
        cls.filenames = [file.name for file in cls.files]
        for contents, file in zip(cls.contents, cls.files):
            file.write(contents)
            file.flush()
        cls.dset = cls.ctx.files(cls.filenames, psize_bytes=1024 * 2, psize_files=None)


    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.tmpdir.cleanup()


    def test_pcount(self):
        file_size = 128 * 8
        file_count = 32
        total_size = file_size * file_count
        part_size = 1024 * 2
        count = round(total_size / part_size)
        self.assertEqual(len(self.dset.parts()), count)


    def test_count(self):
        self.assertEqual(self.dset.count(), 32)


    def test_listings(self):
        dirname = self.tmpdir.name
        filename_pattern = os.path.join(dirname, 'test_file_*.tmp')
        dset = self.ctx.files(filename_pattern)
        self.assertEqual(dset.count(), 32)
        file_count = sum(1 for _ in filter(
                lambda f: os.path.basename(f).startswith('test_file_') and f.endswith('.tmp'),
                self.ctx.files(dirname).filenames
            )
        )
        self.assertEqual(file_count, 32)
        self.assertEqual(self.dset.count(), 32)


    def test_recursive(self):
        filter_test_files = lambda filename: os.path.basename(filename).startswith('test_file_') and filename.endswith('.tmp')
        dirname = os.path.dirname(self.filenames[0])
        dset = self.ctx.files(dirname, True, None, filter_test_files)
        self.assertEqual(dset.count(), 32)
        self.assertEqual(
            sum(1 for _ in filter(
                filter_test_files,
                self.ctx.files(dirname).filenames
            )
        ), 32)


    def test_binary(self):
        self.assertEqual(b''.join(self.dset.values().collect()), b''.join(self.contents))


    def test_decode(self):
        self.assertEqual(''.join(self.dset.decode().values().collect()), ''.join(c.decode() for c in self.contents))
        self.assertEqual(self.dset.decode().values().map(len).collect(), [128 * 8] * 32)


    def test_lines(self):
        self.assertEqual(self.dset.lines().count(), 8 * 32)
        self.assertEqual(self.dset.lines().map(len).collect(), [127] * 8 * 32)
        self.assertEqual(self.dset.lines().collect(),
                         list(chain.from_iterable(c.decode().splitlines() for c in self.contents)))


    def test_decompress(self):
        dset = self.ctx.range(10)
        with TemporaryDirectory() as d:
            strings = dset.map(str)
            gzipped = strings.map_partitions(lambda p: gzip.compress('\n'.join(p).encode()))
            gzipped.collect_as_files(d)
            self.assertEqual(dset.collect(),
                             self.ctx.files(d).decompress().lines().map(int).sort().collect())
