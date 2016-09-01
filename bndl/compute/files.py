from collections import OrderedDict
from functools import partial, lru_cache
from os.path import getsize
import asyncio
import contextlib
import gzip
import logging
import mmap
import os

from bndl.compute.dataset import Dataset, Partition, TransformingDataset
from bndl.net.sendfile import sendfile
from bndl.net.serialize import attach, attachment
from bndl.util import aio
from bndl.util.collection import batch
from bndl.util.fs import filenames
from toolz.itertoolz import pluck
import io


logger = logging.getLogger(__name__)


def _decode(encoding, errors, blob):
    return blob.decode(encoding, errors)


def _splitlines(keepends, blob):
    return blob.splitlines(keepends)


class DistributedFilesOps:
    def decode(self, encoding='utf-8', errors='strict'):
        return self.map_values(partial(_decode, encoding, errors))

    def lines(self, encoding='utf-8', keepends=False, errors='strict'):
        return self.decode(encoding, errors).values().flatmap(partial(_splitlines, keepends))

    def parse_csv(self, **kwargs):
        return self.decode().values().parse_csv(**kwargs)



class DecompressedDistributedFiles(DistributedFilesOps, TransformingDataset):
    pass



class DistributedFiles(DistributedFilesOps, Dataset):
    def __init__(self, ctx, root, recursive=None, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None, split=False):
        '''
        Create a Dataset out of files.

        :param ctx: The ComputeContext
        :param root: str, list
            If str, root is considered to be a file or directory name or a glob pattern (see glob.glob).
            If list, root is considered a list of filenames.
        :param recursive: bool
            Whether to recursively search a (root) directory for files
        :param dfilter: function(dir_name)
            A function to filter out directories by name, return a trueish or falsy
            value to indicate whether to use or the directory or not. 
        :param ffilter: function(file_name)
            A function to filter out files by name, return a trueish or falsy
            value to indicate whether to use or the file or not.
        :param psize_bytes: int or None
            The maximum number of bytes in a partition.
        :param psize_files: int or None
            The maximum number of files in a partition.
        :param split: bool or bytes
            If False, files will not be split to achieve partitions of max.
            size psize_bytes.
            If True, files will be split to achieve partitions of size
            psize_bytes; files will be split to fill each partition.
            If bytes, files will be split just after an occurrence of the given
            string, e.g. a newline.
        '''
        super().__init__(ctx)

        self._root = root
        self._recursive = recursive
        self._dfilter = dfilter
        self._ffilter = ffilter

        if psize_bytes is not None and psize_bytes <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_files is not None and psize_files <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_bytes is None and psize_files is None:
            psize_bytes = 16 * 1024 * 1024
            psize_files = 10 * 1000

        if split and not psize_bytes:
            raise ValueError("sep can't be set without psize_bytes")
        elif isinstance(split, str):
            split = split.encode()

        self.psize_bytes = psize_bytes
        self.psize_files = psize_files
        self.split = split


    def decompress(self):
        decompressed = self.map_values(gzip.decompress)
        decompressed.__class__ = DecompressedDistributedFiles
        return decompressed


    def parse_csv(self, **kwargs):
        file0 = self.filenames[0]
        import pandas as pd
        with open(file0) as f:
            sample = ''.join(f.readline() for _ in range(10))
            kwargs['sample'] = pd.read_csv(io.StringIO(sample), **kwargs)
        return super().parse_csv(**kwargs)


    def parts(self):
        node = self.ctx.node
        assert node.node_type == 'driver'

        if self.psize_bytes:
            batches = self._sliceby_psize()
        else:
            batches = batch(self._files, self.psize_files)

        parts = []
        for idx, files in enumerate(batches):
            if files:
                part = FilesPartition(self, idx)
                parts.append(part)
                node.hosted_values[part.key] = FilesValue(files)

        return parts


    @property
    def cleanup(self):
        def _cleanup(job):
            node = job.ctx.node
            assert node.node_type == 'driver'
            for part in self.parts():
                try:
                    del node.hosted_values[part.key]
                except KeyError:
                    pass

        return _cleanup


    def _sliceby_psize(self):
        psize_bytes = self.psize_bytes
        psize_files = self.psize_files
        split = self.split

        if isinstance(split, str):
            sep = split.encode()
        elif isinstance(split, bytes):
            sep = split
        else:
            sep = None
        if sep:
            sep_len = len(sep)

        batch = []
        batches = [batch]
        space = psize_bytes

        def new_batch():
            nonlocal space, batch, batches
            batch = []
            batches.append(batch)
            space = psize_bytes

        for filename, filesize in self._files:
            if psize_files and len(batch) >= psize_files:
                new_batch()
            if psize_bytes:
                if space < filesize:
                    if split and space > 0:
                        if sep:
                            # split files at sep
                            offset = 0
                            fd = os.open(filename, os.O_RDONLY)
                            try:
                                with mmap.mmap(fd, filesize, access=mmap.ACCESS_READ) as mm:
                                    while offset < filesize:
                                        split = mm.rfind(sep, offset, offset + space) + 1
                                        if split == 0:
                                            if batch:
                                                new_batch()
                                                continue
                                            else:
                                                split = mm.find(sep, offset) + 1
                                                if split == 0:
                                                    split = filesize
                                        length = split - offset
                                        batch.append((filename, offset, length))
                                        offset = split
                                        space -= length
                                        if space < sep_len:
                                            new_batch()
                            finally:
                                os.close(fd)
                        else:
                            # split files anywhere
                            offset = 0
                            remaining = filesize
                            while True:
                                if remaining == 0:
                                    break
                                elif remaining < space:
                                    batch.append((filename, offset, remaining))
                                    space -= remaining
                                    break
                                else:  # remaining > space
                                    batch.append((filename, offset, offset + space))
                                    offset += space
                                    remaining -= space
                                    new_batch()
                        continue
                    else:
                        new_batch()
            batch.append((filename, 0, filesize))
            space -= filesize

        return batches


    @property
    def filenames(self):
        return list(pluck(0, self._files))


    @property
    @lru_cache()
    def _files(self):
        if isinstance(self._root, str):
            return list(filenames(self._root, self._recursive or True, self._dfilter, self._ffilter))

        assert self._recursive == None
        assert self._dfilter == None
        assert self._ffilter == None

        if not isinstance(self._root, (list, tuple)):
            root = list(self._root)
        else:
            root = self._root

        return [
            (filename, getsize(filename))
            for filename in root
        ]


    def __getstate__(self):
        state = dict(self.__dict__)
        for attr in ('_root', '_recursive', '_dfilter', '_ffilter '):
            try:
                del state[attr]
            except KeyError:
                pass
        return state



def _file_attachment(filename, offset, size):
    @contextlib.contextmanager
    def _attacher():
        @asyncio.coroutine
        def sender(loop, writer):
            '''
            make sure there is no data pending in the writer's buffer
            get the socket from the writer
            use sendfile to send file to socket
            '''
            with open(filename, 'rb') as file:
                yield from aio.drain(writer)
                socket = writer.get_extra_info('socket')
                yield from sendfile(socket.fileno(), file.fileno(), offset, size, loop)
        yield size, sender

    return filename.encode('utf-8'), _attacher


class FilesValue(object):
    def __init__(self, files):
        self.files = files


    def __getstate__(self):
        for file in self.files:
            attach(*_file_attachment(*file))
        return {'files': list(pluck(0, self.files))}


    def __setstate__(self, state):
        self.__dict__.update(state)
        # transform list of filenames into ordered dict of filenames and data
        self.files = OrderedDict(
            (filename, attachment(filename.encode('utf-8')))
            for filename in self.files
        )



class FilesPartition(Partition):
    def __init__(self, dset, idx):
        super().__init__(dset, idx)


    @property
    def key(self):
        return str(self.__class__), self.dset.id, self.idx


    def _materialize(self, ctx):
        driver = ctx.node.peers.filter(node_type='driver')[0]
        logger.debug('retrieving files from driver for part %r of data set %r',
                     self.idx, self.dset.id)
        val = driver.get_hosted_value(self.key).result()
        return iter(val.files.items())
