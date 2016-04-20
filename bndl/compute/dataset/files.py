import os.path

from bndl.compute.dataset.base import Dataset, Partition
from bndl.util.fs import filenames, read_file
from bndl.util.collection import batch


class DistributedFiles(Dataset):
    def __init__(self, ctx, root, recursive=False, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None):
        super().__init__(ctx)
        self.filenames = (
            list(filenames(root, recursive, dfilter, ffilter))
            if isinstance(root, str)
            else list(root)
        )

        if psize_bytes is not None and psize_files is not None:
            raise ValueError("can't set both psize_bytes and psize_files")
        elif psize_bytes is not None and psize_bytes <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_files is not None and psize_files <= 0:
            raise ValueError("psize_bytes can't be negative or zero")
        elif psize_bytes is None and psize_files is None:
            psize_bytes = 64 * 1024 * 1024

        self.psize_bytes = psize_bytes
        self.psize_files = psize_files


    def decode(self, encoding='utf-8', errors='strict'):
        return self.map_values(lambda blob: blob.decode(encoding, errors))


    def lines(self, encoding='utf-8', keepends=False, errors='strict'):
        return self.decode(encoding, errors).values().flatmap(lambda blob: blob.splitlines(keepends))


    def parts(self):
        if self.psize_bytes:
            # TODO split within files
            # for start in range(0, len(file), 64):
            #     file[start:start+64]

            part = []
            parts = [part]
            size = 0
            for filename in self.filenames:
                part.append(filename)
                size += os.path.getsize(filename)
                if size >= self.psize:
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


class FilesPartition(Partition):
    def __init__(self, dset, idx, filenames):
        super().__init__(dset, idx)
        self.filenames = filenames

    def __getstate__(self):
        state = dict(self.__dict__)
        # TODO redesign this so that file data can be sent with
        # sendfile or something similar
        state['data'] = list(self._read())
        return state

    def _materialize(self, ctx):
        data = getattr(self, 'data', None)
        if data is None:
            data = self._read()
        return zip(self.filenames, data)

    def _read(self):
        for filename in self.filenames:
            yield read_file(filename, 'rb')
