from itertools import chain, product
from tempfile import TemporaryDirectory
import gzip
import math
import os.path

from bndl.compute.tests import DatasetTest
from bndl.util.text import random_string
from cytoolz import pluck
from os.path import basename


class FilesTest(DatasetTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tmpdir = TemporaryDirectory('bndl_unit_test')
        for idx in range(10):
            with open(os.path.join(cls.tmpdir.name, 'decoy_file_%s.tmp' % idx), 'w') as f:
                print('decoy', file=f)
        cls.line_size = 128
        cls.line_count = 8
        cls.file_size = cls.line_size * cls.line_count
        cls.file_count = 32
        cls.total_line_count = cls.line_count * cls.file_count
        cls.total_size = cls.file_size * cls.file_count
        cls.contents = [
            ('\n'.join(random_string(cls.line_size - 1) for _ in range(cls.line_count)) + '\n').encode()
            for _ in range(cls.file_count)
        ]
        cls.files = [
            open(os.path.join(cls.tmpdir.name, 'test_file_%s.tmp' % idx), 'wb')
            for idx in range(len(cls.contents))
        ]
        cls.filenames = [file.name for file in cls.files]
        for contents, file in zip(cls.contents, cls.files):
            file.write(contents)
            file.flush()
        cls.psize_bytes = cls.file_size * 2
        cls.dset = cls.ctx.files(cls.filenames, psize_bytes=cls.psize_bytes, psize_files=None).cache()


    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.tmpdir.cleanup()


    def test_pcount(self):
        count = round(self.total_size / self.psize_bytes)
        self.assertEqual(len(self.dset.parts()), count)


    def _test_psize(self, psize_bytes, psize_files, dset):
        data = dset.values()
        if psize_bytes:
            self.assertTrue(all(data.map(len).map_partitions(lambda p: (sum(p) <= psize_bytes,)).collect()))
        if psize_files:
            self.assertTrue(all(data.map_partitions(lambda p: (sum(1 for _ in p) <= psize_files,)).collect()))


    def test_psize(self):
        psize_bytes = [None, self.file_size * 3]
        psize_files = [None, 3]

        for psize_bytes, psize_files in product(psize_bytes, psize_files):
            with self.subTest('psize_bytes = %s, psize_files = %s' % (psize_bytes, psize_files)):
                self._test_psize(psize_bytes, psize_files,
                                 self.ctx.files(self.filenames, psize_bytes=psize_bytes, psize_files=psize_files))


    def test_count(self):
        self.assertEqual(self.dset.count(), self.file_count)


    def test_listings(self):
        dirname = self.tmpdir.name
        filename_pattern = os.path.join(dirname, 'test_file_*.tmp')
        dset = self.ctx.files(filename_pattern)
        self.assertEqual(dset.count(), self.file_count)
        file_count = sum(1 for _ in filter(
                lambda f: os.path.basename(f).startswith('test_file_') and f.endswith('.tmp'),
                self.ctx.files(dirname).filenames
            )
        )
        self.assertEqual(file_count, self.file_count)
        self.assertEqual(self.dset.count(), self.file_count)


    def test_recursive(self):
        filter_test_files = lambda filename: os.path.basename(filename).startswith('test_file_') and filename.endswith('.tmp')
        dirname = os.path.dirname(self.filenames[0])
        for loc in ('driver', 'workers'):
            dset = self.ctx.files(dirname, True, None, filter_test_files, location=loc).cache()
            self.assertEqual(dset.count(), self.file_count)
            self.assertEqual(
                sum(1 for _ in filter(
                    filter_test_files,
                    self.ctx.files(dirname).filenames
                )
            ), self.file_count)
            files = sorted(dset.icollect(), key=lambda file: int(basename(file[0]).replace('test_file_', '').replace('.tmp', '')))
            self.assertEqual(b''.join(pluck(1, files)), b''.join(self.contents))


    def test_binary(self):
        self.assertEqual(b''.join(self.dset.values().collect()), b''.join(self.contents))


    def test_decode(self):
        self.assertEqual(''.join(self.dset.decode().values().collect()), ''.join(c.decode() for c in self.contents))
        self.assertEqual(self.dset.decode().values().map(len).collect(), [self.file_size] * self.file_count)


    def test_lines(self):
        self.assertEqual(self.dset.lines().count(), self.total_line_count)
        self.assertEqual(self.dset.lines().map(len).collect(), [self.line_size - 1] * self.total_line_count)
        self.assertEqual(self.dset.lines().collect(),
                         list(chain.from_iterable(c.decode().splitlines() for c in self.contents)))


    def test_decompress(self):
        dset = self.ctx.range(10)
        with TemporaryDirectory() as d:
            strings = dset.map(str)
            gzipped = strings.glom().map(lambda p: gzip.compress('\n'.join(p).encode()))
            gzipped.collect_as_files(d)
            self.assertEqual(dset.collect(),
                             self.ctx.files(d).decompress().lines().map(int).sort().collect())


    def test_split_files(self):
        for factor in (1.9, 2.0, 2.1):
            with self.subTest('factor is %r' % factor):
                psize_bytes = int(self.psize_bytes / 2 * factor)
                dset = self.ctx.files(self.filenames, psize_bytes=psize_bytes, psize_files=None, split=True).cache()
                for psize in dset.values().map(len).map_partitions(lambda p: [sum(p)]).collect():
                    self.assertLessEqual(psize, psize_bytes)
                self.assertEqual(len(dset.parts()), math.ceil(self.total_size / psize_bytes))
                self.assertEqual(b''.join(dset.values().collect()), b''.join(self.contents))


    def test_split_fileslines(self):
        sep = '\n'
        for factor in (1.8, 1.95, 2.0, 2.2):
            with self.subTest('factor is %r' % factor):
                psize_bytes = int(self.psize_bytes / 2 * factor)
                dset = self.ctx.files(self.filenames, psize_bytes=psize_bytes, psize_files=None, split=sep).cache()
                self.assertEqual(b''.join(dset.values().collect()), b''.join(self.contents))
                max_lines_per_part = psize_bytes // self.line_size
                for part in dset.values().collect(parts=True)[:-1]:
                    self.assertEqual(b''.join(part).decode().count(sep), max_lines_per_part)
