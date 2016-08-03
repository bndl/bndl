import glob
import os.path

import scandir


def filenames(root, recursive=False, dfilter=None, ffilter=None):
    '''
    Filter file names from a root directory.
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
    for name in glob.glob(root):
        if os.path.isfile(name):
            if not ffilter or ffilter(name):
                yield name
        else:
            yield from _filenames(root, recursive, dfilter, ffilter)


def _filenames(directory, recursive=False, dfilter=None, ffilter=None):
    try:
        scan = scandir.scandir(directory)
    except PermissionError:
        pass
    else:
        for entry in scan:
            epath = os.path.join(directory, entry.name)
            if entry.is_dir() and recursive and (not dfilter or dfilter(epath)):
                yield from _filenames(epath, True, dfilter, ffilter)
            elif entry.is_file() and (not ffilter or ffilter(epath)):
                yield epath


def listdirabs(path):
    return (os.path.join(path, fname) for fname in os.listdir(path))


def read_file(filename, mode='rb'):
    with open(filename, mode) as file:
        return file.read()
