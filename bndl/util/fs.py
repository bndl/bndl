import glob
import os.path


def filenames(root, recursive=False, dfilter=None, ffilter=None):
    '''
    Filter file names from a root directory.
    :param root: str
        A root directory. May be a glob pattern as supported by glob.glob
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
        if os.path.isfile(name) and (not ffilter or ffilter(name)):
            yield name
        elif recursive:
            for directory, _, fnames in os.walk(name):
                if not dfilter or dfilter(directory):
                    for fname in fnames:
                        fpath = os.path.join(directory, fname)
                        if not ffilter or ffilter(fpath):
                            yield fpath
        else:
            for fname in os.listdir(name):
                fpath = os.path.join(name, fname)
                if os.path.isfile(fpath) and (not ffilter or ffilter(fpath)):
                    yield fpath


def listdirabs(path):
    return (os.path.join(path, fname) for fname in os.listdir(path))


def read_file(filename, mode='rb'):
    with open(filename, mode) as file:
        return file.read()
