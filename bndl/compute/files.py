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

from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
from itertools import chain, cycle
from os import stat, posix_fadvise, POSIX_FADV_SEQUENTIAL
from os.path import getsize, join, isfile
from queue import Queue, Empty
import glob
import gzip
import io
import logging
import mmap
import os.path
import struct
import sys

from cytoolz import pluck, interleave
import scandir

from bndl.compute.dataset import Dataset, Partition, TransformingDataset, NODE_LOCAL
from bndl.execute.worker import current_worker
from bndl.net.sendfile import file_attachment
from bndl.net.serialize import attach, attachment
from bndl.util import collection
from bndl.util import serialize
import lz4
import marisa_trie


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
        return _driver_files(ctx, root, recursive, dfilter, ffilter, psize_bytes, psize_files, split)


def _ensure_min_pcount(batches, min_pcount):
    while len(batches) < min_pcount:
        splits = 0
        for batch in list(batches):
            if len(batch) > 1:
                idx = len(batch) // 2
                batches.append(list(batch[idx:]))
                del batch[idx:]
                splits += 1
                if len(batches) >= min_pcount:
                    break
        if not splits:
            break


def _batches(root, recursive=True, dfilter=None, ffilter=None, psize_bytes=None, psize_files=None, min_pcount=None, split=False):
    if isinstance(root, str):
        root = os.path.expanduser(root)
        filesizes = list(_filesizes(root, recursive, dfilter, ffilter))
    else:
        filesizes = [(filename, getsize(filename)) for filename in root]
        if not all(map(isfile, pluck(0, filesizes))):
            raise ValueError('Not every file in %r is a file' % root)
    # batch in chunks by size / file count
    batches = _batch_files(filesizes, psize_bytes, psize_files, split)
    # ensure len(batches) >= min_pcount
    if min_pcount:
        _ensure_min_pcount(batches, min_pcount)
    # compact filenames into a trie
    return [marisa_trie.RecordTrie('QQ', batch) for batch in batches]


def _driver_files(ctx, root, recursive, dfilter, ffilter, psize_bytes, psize_files, split):
    batches = _batches(root, recursive, dfilter, ffilter, psize_bytes, psize_files, ctx.default_pcount, split)
    batches = list(zip(cycle((ctx.node.ip_addresses(),)), batches))
    logger.debug('created files dataset of %s batches', len(batches))
    return FilesDataset(ctx, batches, split)


def _ip_addresses(worker):
    return tuple(sorted(map(str, worker.ip_addresses())))


def _worker_files(ctx, root, recursive, dfilter, ffilter, psize_bytes, psize_files, split):
    batch_requests = []
    seen = set()
    ctx.await_workers()
    for worker in ctx.workers:
        ips = tuple(sorted(worker.ip_addresses()))
        if ips not in seen:
            seen.add(ips)
            batch_requests.append((worker, worker.service('tasks')
                                                 .execute(_batches, root, recursive, dfilter,
                                                          ffilter, psize_bytes, psize_files,
                                                          None, split)))

    batches = []
    for worker, batch_request in batch_requests:
        worker_batches = []
        batches.append(worker_batches)
        for file_chunks in batch_request.result():
            worker_batches.append((worker.ip_addresses(), file_chunks))

    # interleave batches to ease scheduling overhead
    batches = list(interleave(batches))

    logger.debug('created files dataset of %s batches accross %s worker nodes',
                 len(batches), len(batch_requests))

    return FilesDataset(ctx, batches, split)


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

    if dfilter:
        dfilter = serialize.dumps(dfilter)
    if ffilter:
        ffilter = serialize.dumps(ffilter)

    scan_func = partial(_scan_dir_worker, recursive=recursive, dfilter=dfilter, ffilter=ffilter)
    pool_size = min(16, max(4, os.cpu_count()))

    with ProcessPoolExecutor(pool_size) as executor:
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


def _scan_dir_worker(directory, recursive, dfilter, ffilter):
    if dfilter:
        dfilter = serialize.loads(*dfilter)
    if ffilter:
        ffilter = serialize.loads(*ffilter)

    subdirs = [directory]
    fnames = []
    while len(fnames) < 10000:
        try:
            subdir = subdirs.pop()
        except IndexError:
            break
        else:
            more_subdirs, more_fnames = _scan_dir(subdir, recursive, dfilter, ffilter)
            subdirs.extend(more_subdirs)
            fnames.extend(more_fnames)
    return subdirs, fnames


def _scan_dir(directory, recursive, dfilter, ffilter):
    subdirs = []
    fnames = []

    try:
        scan = scandir.scandir(directory)
    except PermissionError:
        return (), ()

    dir_fd = os.open(directory, os.O_RDONLY)

    try:
        for entry in scan:
            epath = join(directory, entry.name)
            if entry.is_dir() and recursive and (not dfilter or dfilter(epath)):
                subdirs.append(epath)
            elif entry.is_file() and (not ffilter or ffilter(epath)):
                fnames.append((epath, stat(entry.name, dir_fd=dir_fd).st_size))
    finally:
        os.close(dir_fd)

    return subdirs, fnames


def _batch_files(filesizes, psize_bytes, psize_files, split):
    if not psize_bytes:
        with_offset = ((file, (0, size)) for file, size in filesizes)
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
        if psize_bytes and space < filesize:
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
                                split = mm.rfind(sep, offset, offset + space) + sep_len
                                if split < sep_len:
                                    if batch:
                                        new_batch()
                                        continue
                                    else:
                                        split = mm.find(sep, offset) + sep_len
                                        if split < sep_len:
                                            split = filesize
                                length = split - offset
                                batch.append((filename, (offset, length)))
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
                            batch.append((filename, (offset, remaining)))
                            space -= remaining
                            break
                        else:  # remaining > space
                            batch.append((filename, (offset, offset + space)))
                            offset += space
                            remaining -= space
                            new_batch()
                continue
            elif batch:
                new_batch()
        batch.append((filename, (0, filesize)))
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
    def __init__(self, ctx, batches, split):
        super().__init__(ctx)
        self.split = bool(split)
        self._parts = [
            FilesPartition(self, idx, file_chunks, location)
            for idx, (location, file_chunks) in enumerate(batches)
        ]


    def lines(self, encoding='utf-8', errors='strict', keepends=True):
        if not self.split and keepends:
            return LinesDataset(self, encoding, errors)
        else:
            return super().lines(encoding, errors, keepends)


    def decompress(self, compression='gzip'):
        if compression == 'gzip':
            decompress = gzip.decompress
        elif compression == 'lz4':
            decompress = lz4.decompress
        elif compression is not None:
            raise ValueError('Compression %r not supported' % compression)
        decompressed = self.map_values(decompress)
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
        return chain.from_iterable(part.file_chunks.items() for part in self._parts)


    @property
    def filenames(self):
        return list(pluck(0, self._chunks))


    @property
    def filecount(self):
        return sum(len(part.file_chunks) for part in self._parts)


    @property
    def size(self):
        return sum(pluck(1, pluck(1, self._chunks)))


    def __getstate__(self):
        state = super().__getstate__()
        try:
            del state['_parts']
        except KeyError:
            pass
        return state



class FilesPartition(Partition):
    def __init__(self, dset, idx, file_chunks, location):
        super().__init__(dset, idx)
        self.file_chunks = file_chunks
        self.location = location


    def _locality(self, workers):
        if self.location:
            local_ips = self.location
            for worker in workers:
                if worker.ip_addresses() & local_ips:
                    yield worker, NODE_LOCAL


    def _local(self):
        for filename, (offset, size) in self.file_chunks.items():
            with open(filename, 'rb') as f:
                f.seek(offset)
                contents = f.read(size)
            yield filename, contents


    def _remote(self):
        for node in self.dset.ctx.node.peers.values():
            if node.ip_addresses() & self.location:
                request = node.service('tasks').execute(_RemoteFilesSender, self.file_chunks)
                contents = request.result().data
                assert len(contents) == len(self.file_chunks)
                return zip(self.file_chunks.keys(), contents)
        raise RuntimeError('Source (IP %s) not available for %s' % (self.location, self))


    def _compute(self):
        if self.location is None or self.location & current_worker().ip_addresses():
            return self._local()
        else:
            return self._remote()



class LinesDataset(Dataset):
    def __init__(self, src, encoding, errors):
        super().__init__(src.ctx, src)
        self.encoding = encoding
        self.errors = errors
        self._parts = [
            LinesPartition(self, part.idx, part.file_chunks, part.location)
            for part in src.parts()
        ]


    def parts(self):
        return self._parts



class LinesPartition(FilesPartition):
    def _local(self):
        if not self.dset.encoding:
            open_file = partial(open, mode='rb')
        else:
            open_file = partial(open, encoding=self.dset.encoding, errors=self.dset.errors)

        for filename in self.file_chunks:
            with open_file(filename) as f:
                yield from f


    def _remote(self):
        contents = pluck(1, super()._remote())
        if self.dset.encoding is not None:
            contents = map(partial(_decode, self.dset.encoding, self.dset.errors), contents)
        lines = map(partial(_splitlines, True), contents)
        return chain.from_iterable(lines)



class _RemoteFilesSender(object):
    def __init__(self, file_chunks):
        self.file_chunks = file_chunks


    def __getstate__(self):
        prefix = id(self)
        for idx, (filename, (offset, size)) in enumerate(self.file_chunks.items()):
            try:
                fd = os.open(filename, 'rb')
                posix_fadvise(fd, offset, size, POSIX_FADV_SEQUENTIAL)
                os.close(fd)
            except Exception:
                pass
            _, attacher = file_attachment(filename, offset, size, False)
            key = struct.pack('NN', prefix, idx)
            attach(key, attacher)
        return {'prefix': prefix, 'N': idx + 1}


    def __setstate__(self, state):
        prefix = state['prefix']
        N = state['N']
        self.data = [
            attachment(struct.pack('NN', prefix, idx))
            for idx in range(N)
        ]
