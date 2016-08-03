from itertools import chain
import os.path
from tempfile import NamedTemporaryFile

from bndl.compute.dataset.tests import DatasetTest
from bndl.util.text import random_string


def filter_test_files(filename):
    return os.path.basename(filename).startswith('bndl_unit_test_') and filename.endswith('.tmp')


class FilesTest(DatasetTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.contents = [('\n'.join(random_string(127) for _ in range(8)) + '\n').encode() for _ in range(32)]
        cls.files = [NamedTemporaryFile(prefix='bndl_unit_test_', suffix='.tmp') for _ in cls.contents]
        cls.filenames = [file.name for file in cls.files]
        for contents, file in zip(cls.contents, cls.files):
            file.file.write(contents)
            file.file.flush()
        cls.dset = cls.ctx.files(cls.filenames, psize_bytes=1024 * 2, psize_files=None)


    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for f in cls.files:
            f.close()


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
        dirname = os.path.dirname(self.filenames[0])
        filename_pattern = os.path.join(dirname, 'bndl_unit_test_*.tmp')
        dset = self.ctx.files(filename_pattern)
        self.assertEqual(dset.count(), 32)
        file_count = sum(1 for _ in filter(
                lambda f: os.path.basename(f).startswith('bndl_unit_test_') and f.endswith('.tmp'),
                self.ctx.files(dirname).filenames
            )
        )
        self.assertEqual(file_count, 32)
        self.assertEqual(self.dset.count(), 32)


    def test_recursive(self):
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
