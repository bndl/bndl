import os.path

from bndl.compute.dataset.base import Dataset, Partition
from bndl.util.fs import filenames, read_file
from bndl.util.collection import batch
from bndl.net.serialize import attach, attachment
import asyncio
from bndl.net.sendfile import sendfile
from collections import OrderedDict
from bndl.util import aio
import sys


class DistributedFiles(Dataset):
    def __init__(self, ctx, root, recursive=False, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None):
        super().__init__(ctx)
        self.filenames = (
            list(filenames(root, recursive, dfilter, ffilter))
            if isinstance(root, str)
            else list(root)
        )

        if psize_bytes is not None and psize_bytes <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_files is not None and psize_files <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_bytes is None and psize_files is None:
            psize_bytes = 16 * 1024 * 1024
            psize_files = 1000

        self.psize_bytes = psize_bytes
        self.psize_files = psize_files


    def decode(self, encoding='utf-8', errors='strict'):
        return self.map_values(lambda blob: blob.decode(encoding, errors))


    def lines(self, encoding='utf-8', keepends=False, errors='strict'):
        return self.decode(encoding, errors).values().flatmap(lambda blob: blob.splitlines(keepends))


    def parts(self):
        if self.psize_bytes:
            psize_bytes = self.psize_bytes
            psize_files = self.psize_files or sys.maxsize

            # TODO split within files
            # for start in range(0, len(file), 64):
            #     file[start:start+64]

            part = []
            parts = [part]
            size = 0
            for filename in self.filenames:
                part.append(filename)
                size += os.path.getsize(filename)
                if size > psize_bytes or len(part) > psize_files:
                    part = []
                    parts.append(part)
                    size = 0
        else:
            parts = batch(self.filenames, self.psize_files)


        return [
            FilesPartition(self, idx, filenames)
            for idx, filenames in enumerate(parts)
            if filenames
        ]


def _file_attachment(filename):
    # get the size of the file
    size = os.stat(filename).st_size
    # the attachment sender:
    @asyncio.coroutine
    def sender(writer):
        # make sure there is no data pending in the writer's buffer
        yield from aio.drain(writer)
        # get the socket from the writer
        socket = writer.get_extra_info('socket')
        # open the file for binary reading
        file = open(filename, 'rb')
        try:
            # use sendfile to send file to socket
            yield from sendfile(socket.fileno(), file.fileno(), 0, size)
        finally:
            # close the file to clean up
            file.close()
    # return the key, the size and the attachment sender
    return filename.encode('utf-8'), size, sender


class FilesPartition(Partition):
    def __init__(self, dset, idx, filenames):
        super().__init__(dset, idx)
        self.filenames = filenames

    def __getstate__(self):
        state = dict(self.__dict__)
        if 'data' not in state:
            for filename in self.filenames:
                attach(*_file_attachment(filename))
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.data = OrderedDict()
        for filename in self.filenames:
            a = attachment(filename.encode('utf-8'))
            self.data[filename] = a

    def _materialize(self, ctx):
        data = getattr(self, 'data', {})
        if data:
            return iter(data.items())
        else:
            data = OrderedDict(zip(self.filenames, self._read()))

    def _read(self):
        for filename in self.filenames:
            yield read_file(filename, 'rb')
