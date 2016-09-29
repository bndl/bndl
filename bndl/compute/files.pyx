from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
from itertools import groupby, chain
from os import stat
from os.path import getsize, join
from queue import Queue, Empty
import glob
import gzip
import io
import logging
import mmap
import os.path
import sys

from bndl.compute.dataset import Dataset, Partition, TransformingDataset
from bndl.net.sendfile import file_attachment
from bndl.net.serialize import attach, attachment
from bndl.util import collection
from bndl.util import serialize
from cytoolz.itertoolz import pluck
import scandir
from operator import attrgetter


logger = logging.getLogger(__name__)


def files(ctx, root, recursive=True, dfilter=None, ffilter=None,
          psize_bytes=None, psize_files=None, split=False, location='driver'):
    '''
    Create a Dataset out of files.

    :param ctx: The ComputeContext
    :param root: str, list
        If str, root is considered to be a file or directory name or a glob
        pattern (see glob.glob).
        If list, root is considered a list of filenames.
    :param recursive: bool
        Whether to recursively search a (root) directory for files, defaults
        to True.
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
    :param location: str
        Use 'driver' to locate the files on the driver machine or use 'workers'
        to locate them on the worker machines. In the later case one worker per
        ip address will be selected and it scans the local directory. This
        requires root to be a str.
    '''
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

    if location == 'workers':
        assert isinstance(root, str)
        return _worker_files(ctx, root, recursive, dfilter, ffilter, psize_bytes, psize_files, split)
    else:
        batches = _batches(root, recursive, dfilter, ffilter, psize_bytes, psize_files, split)
        return RemoteFilesDataset(ctx, batches)


def _batches(root, recursive=True, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None, split=False):
    if isinstance(root, str):
        filesizes = list(_filesizes(root, recursive, dfilter, ffilter))
    else:
        filesizes = [(filename, getsize(filename)) for filename in root]
    return _batch_files(filesizes, psize_bytes, psize_files, split)


def _get_batches(worker, *args, **kwargs):
    return _batches(*args, **kwargs)

def _ip_addresses(worker):
    return tuple(sorted(map(str, worker.ip_addresses)))

def _worker_files(ctx, root, recursive, dfilter, ffilter, psize_bytes, psize_files, split):
    batch_requests = []
    workers = sorted(ctx.workers, key=_ip_addresses)
    for _, workers in groupby(workers, key=_ip_addresses):
        worker = next(workers)
        batch_requests.append((worker, worker.run_task(_get_batches, root, recursive, dfilter,
                                                       ffilter, psize_bytes, psize_files, split)))

    batches = []
    for worker, batch_request in batch_requests:
        worker_batches = batch_request.result()
        for file_chunks in worker_batches:
            batches.append((worker.ip_addresses, file_chunks))

    return LocalFilesDataset(ctx, batches, split)


def _filesizes(root, recursive=True, dfilter=None, ffilter=None):
    '''
    Filter file names and their sizes from a root directory.

    :param root: str
        A root directory name or glob pattern as supported by glob.glob
    :param recursive: bool
        Whether to recurse into sub directories.
    :param dfilter: callable
        A function to return True if a directory is to be traversed, or False
        otherwise. Only used when recursing into sub directories.
    :param ffilter: callable
        A function to return True if a file is to be yielded, or False
        otherwise.
    '''
    # scan for files and sub-directories given the root directory / glob pattern
    subdirs = []
    if sys.version_info >= (3, 5):
        names = glob.glob(root, recursive=recursive)
    else:
        names = glob.glob(root)
    for name in names:
        if os.path.isfile(name):
            if not ffilter or ffilter(name):
                yield name, getsize(name)
        else:
            subdirs.append(name)
    # scan sub-directories concurrently if > 1
    if dfilter:
        dfilter = serialize.dumps(dfilter)
    if ffilter:
        ffilter = serialize.dumps(ffilter)

    pool_size = max(4, os.cpu_count())

    with ProcessPoolExecutor(pool_size) as executor:
        scan_func = partial(_scan_dir, recursive=recursive, dfilter=dfilter, ffilter=ffilter)
        scans = Queue()
        for subdir in subdirs:
            scans.put(executor.submit(scan_func, subdir))
        while True:
            try:
                dnames, fnames = scans.get_nowait().result()
            except Empty:
                break
            else:
                for dname in dnames:
                    scans.put(executor.submit(scan_func, dname))
                if fnames:
                    yield from fnames


def _scan_dir(directory, recursive=False, dfilter=None, ffilter=None):
    if dfilter:
        dfilter = serialize.loads(*dfilter)
    if ffilter:
        ffilter = serialize.loads(*ffilter)

    subdirs = [directory]
    fnames = []
    while True:
        try:
            subdir = subdirs.pop()
        except IndexError:
            break

        try:
            scan = scandir.scandir(subdir)
        except PermissionError:
            continue

        dir_fd = os.open(subdir, os.O_RDONLY)

        try:
            for entry in scan:
                epath = join(subdir, entry.name)
                if entry.is_dir() and recursive and (not dfilter or dfilter(epath)):
                    subdirs.append(epath)
                elif entry.is_file() and (not ffilter or ffilter(epath)):
                    fnames.append((epath, stat(entry.name, dir_fd=dir_fd).st_size))
        finally:
            os.close(dir_fd)
    return subdirs, fnames



def _batch_files(filesizes, psize_bytes, psize_files, split):
    if not psize_bytes:
        with_offset = ((file, 0, size) for file, size in filesizes)
        return collection.batch(with_offset, psize_files)

    if isinstance(split, str):
        sep = split.encode()
    elif isinstance(split, bytes):
        sep = split
    else:
        sep = None
    sep_len = len(sep) if sep else 0

    batch = []
    batches = [batch]
    space = psize_bytes

    def new_batch():
        nonlocal space, batch, batches
        batch = []
        batches.append(batch)
        space = psize_bytes

    for filename, filesize in filesizes:
        if psize_files and len(batch) >= psize_files:
            new_batch()
        if psize_bytes:
            if space < filesize:
                if split:
                    if space == 0:
                        new_batch()
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


def _decode(encoding, errors, blob):
    return blob.decode(encoding, errors)


def _splitlines(keepends, blob):
    return blob.splitlines(keepends)



class DistributedFilesOps:
    def decode(self, encoding='utf-8', errors='strict'):
        return self.map_values(partial(_decode, encoding, errors))

    def lines(self, encoding='utf-8', errors='strict', keepends=True):
        data = self if encoding is None else self.decode(encoding, errors)
        return data.values().flatmap(partial(_splitlines, keepends))

    def parse_csv(self, **kwargs):
        return self.decode().values().parse_csv(**kwargs)



class DecompressedFilesDataset(DistributedFilesOps, TransformingDataset):
    pass



class FilesDataset(DistributedFilesOps, Dataset):
    def __init__(self, ctx):
        super().__init__(ctx)


    def decompress(self, format='gzip'):
        assert format == 'gzip'
        decompressed = self.map_values(gzip.decompress)
        decompressed.__class__ = DecompressedFilesDataset
        return decompressed


    def parse_csv(self, **kwargs):
        file0 = self.filenames[0]
        import pandas as pd
        with open(file0) as f:
            sample = ''.join(f.readline() for _ in range(10))
            kwargs['sample'] = pd.read_csv(io.StringIO(sample), **kwargs)
        return super().parse_csv(**kwargs)


    def parts(self):
        return self._parts


    @property
    def _chunks(self):
        return chain.from_iterable(part.file_chunks for part in self._parts)


    @property
    def filenames(self):
        return list(pluck(0, self._chunks))


    @property
    def size(self):
        return sum(pluck(2, self._chunks))


    def __getstate__(self):
        state = dict(self.__dict__)
        try:
            state['_parts']
        except KeyError:
            pass
        return state



class LocalFilesDataset(FilesDataset):
    def __init__(self, ctx, batches, split):
        super().__init__(ctx)
        self.split = bool(split)
        self._parts = [
            LocalFilesPartition(self, idx, ip_addresses, file_chunks)
            for idx, (ip_addresses, file_chunks) in enumerate(batches)
        ]


    def lines(self, encoding='utf-8', errors='strict', keepends=True):
        if not self.split and keepends:
            ds = FilesDataset(self.ctx)
            ds._parts = [
                LinesFilesPartition(ds, part.idx, part.file_chunks, encoding, errors)
                for part in self._parts
            ]
            return ds
        else:
            return super().lines(encoding, errors, keepends)



class RemoteFilesDataset(FilesDataset):
    def __init__(self, ctx, batches):
        super().__init__(ctx)
        self._parts = [
            RemoteFilesPartition(self, idx, file_chunks)
             for idx, file_chunks in enumerate(batches)
             if file_chunks
        ]



class FilesPartition(Partition):
    def __init__(self, dset, idx, file_chunks):
        super().__init__(dset, idx)
        self.file_chunks = file_chunks


    def _materialize(self, ctx):
        for filename, offset, size in self.file_chunks:
            with open(filename, 'rb') as f:
                f.seek(offset)
                contents = f.read(size)
            yield filename, contents



class LinesFilesPartition(FilesPartition):
    def __init__(self, dset, idx, file_chunks, encoding, errors):
        super().__init__(dset, idx, file_chunks)
        self.encoding = encoding
        self.errors = errors


    def _materialize(self, ctx):
        if not self.encoding:
            open_file = partial(open, mode='rb')
        else:
            open_file = partial(open, encoding=self.encoding, errors=self.errors)

        for filename, offset, size in self.file_chunks:
            with open_file(filename) as f:
                yield from f



class MaterializedFilesPartition(FilesPartition):
    def _materialize(self, ctx):
        return self.files_data



class LocalFilesPartition(FilesPartition):
    def __init__(self, dset, idx, ip_addresses, file_chunks):
        super().__init__(dset, idx, file_chunks)
        self.ip_addresses = ip_addresses


    def allowed_workers(self, workers):
        return [worker for worker in workers
                if worker.ip_addresses & self.ip_addresses]



class RemoteFiles(object):
    def __init__(self, file_chunks):
        self.file_chunks = file_chunks


    def __getstate__(self):
        for chunk in self.file_chunks:
            attach(*file_attachment(*chunk))
        state = dict(self.__dict__)
        state['files_names'] = list(pluck(0, self.file_chunks))
        del state['file_chunks']
        return state


    def __setstate__(self, state):
        files_names = state.pop('files_names')
        self.data = [
            (filename, attachment(filename.encode('utf-8')))
            for filename in files_names
        ]


def _get_chunks(worker, file_chunks):
    return RemoteFiles(file_chunks)


class RemoteFilesPartition(FilesPartition):
    def __init__(self, dset, idx, file_chunks):
        super().__init__(dset, idx, file_chunks)
        self.source = dset.ctx.node.name


    def _materialize(self, ctx):
        request = ctx.node.peers[self.source].run_task(_get_chunks, self.file_chunks)
        return request.result().data
