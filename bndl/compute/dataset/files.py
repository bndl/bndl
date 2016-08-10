from collections import OrderedDict
from functools import partial, lru_cache
from os.path import getsize
from os import fstat
import asyncio
import contextlib
import gzip
import logging
import sys

from bndl.compute.dataset.base import Dataset, Partition, TransformingDataset
from bndl.net.sendfile import sendfile
from bndl.net.serialize import attach, attachment
from bndl.util import aio
from bndl.util.collection import batch
from bndl.util.exceptions import catch
from bndl.util.fs import filenames
from toolz.itertoolz import pluck


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



class DecodedDistributedFiles(TransformingDataset, DistributedFilesOps):
    pass



class DistributedFiles(Dataset, DistributedFilesOps):
    def __init__(self, ctx, root, recursive=None, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None):
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

        self.psize_bytes = psize_bytes
        self.psize_files = psize_files


    def decompress(self):
        decompressed = self.map_values(gzip.decompress)
        decompressed.__class__ = DecodedDistributedFiles
        return decompressed


    def parts(self):
        node = self.ctx.node
        assert node.node_type == 'driver'

        if self.psize_bytes:
            batches = self._sliceby_psize()
        else:
            batches = batch(self.filenames, self.psize_files)

        parts = []
        for idx, filenames in enumerate(batches):
            if filenames:
                part = FilesPartition(self, idx)
                parts.append(part)
                node.hosted_values[part.key] = FilesValue(filenames)

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
        psize_files = self.psize_files or sys.maxsize

        # TODO split within files
        # for start in range(0, len(file), 64):
        #     file[start:start+64]

        batch = []
        batches = [batch]
        size = 0

        for filename, filesize in self.filenames_and_sizes:
            batch.append(filename)
            size += filesize
            if size >= psize_bytes or len(batch) >= psize_files:
                batch = []
                batches.append(batch)
                size = 0

        return batches


    @property
    def filenames(self):
        return list(pluck(0, self.filenames_and_sizes))


    @property
    @lru_cache()
    def filenames_and_sizes(self):
        if isinstance(self._root, str):
            return list(filenames(self._root, self._recursive, self._dfilter, self._ffilter))

        assert self._recursive == None
        assert self._dfilter == None
        assert self._ffilter == None

        if not isinstance(self._root, (list, tuple)):
            root = list(self._root)
        else:
            root = self._root
        return [(filename, getsize(filename))
                for filename in root]


    def __getstate__(self):
        state = dict(self.__dict__)
        for attr in ('_root', '_recursive', '_dfilter', '_ffilter '):
            try:
                del state[attr]
            except KeyError:
                pass
        return state



def _file_attachment(filename):
    @contextlib.contextmanager
    def _attacher():
        file = open(filename, 'rb')
        size = fstat(file.fileno()).st_size

        @asyncio.coroutine
        def sender(loop, writer):
            '''
            make sure there is no data pending in the writer's buffer
            get the socket from the writer
            use sendfile to send file to socket
            '''
            yield from aio.drain(writer)
            socket = writer.get_extra_info('socket')
            yield from sendfile(socket.fileno(), file.fileno(), 0, size, loop)

        try:
            yield size, sender
        finally:
            with catch():
                file.close()

    return filename.encode('utf-8'), _attacher


class FilesValue(object):
    def __init__(self, filenames):
        self.filenames = filenames


    def __getstate__(self):
        state = dict(self.__dict__)
        for filename in self.filenames:
            attach(*_file_attachment(filename))
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.files = OrderedDict(
            (filename, attachment(filename.encode('utf-8')))
            for filename in self.filenames
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
