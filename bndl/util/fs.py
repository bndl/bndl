from concurrent.futures.process import ProcessPoolExecutor
from functools import partial
import glob
from os import stat
from os.path import getsize, join
import os.path
from queue import Queue, Empty

from bndl.util import serialize
import scandir


def filenames(root, recursive=True, dfilter=None, ffilter=None):
    '''
    Filter file names from a root directory and yield their paths and size as tuples.

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
    for name in glob.glob(root):
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
    while len(fnames) < 10000:
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


def listdirabs(path):
    return (os.path.join(path, fname) for fname in os.listdir(path))


def read_file(filename, mode='rb'):
    with open(filename, mode) as file:
        return file.read()
