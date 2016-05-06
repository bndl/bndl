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
            for directory, _, filenames in os.walk(name):
                if not dfilter or dfilter(directory):
                    for filename in filenames:
                        filepath = os.path.join(directory, filename)
                        if not ffilter or ffilter(filepath):
                            yield filepath
        else:
            for filename in os.listdir(name):
                filepath = os.path.join(name, filename)
                if os.path.isfile(filepath) and (not ffilter or ffilter(filepath)):
                    yield filepath

def listdirabs(path):
    return (os.path.join(path, fname) for fname in os.listdir(path))

def read_file(filename, mode='rb'):
    with open(filename, mode) as f:
        return f.read()
