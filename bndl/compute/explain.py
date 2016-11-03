from collections import Iterable
from functools import wraps
import contextlib
import inspect
import os.path
import threading
import traceback


_callsite = threading.local()


def _get_callsite(*internal, name=None):
    stack = traceback.extract_stack()

    internal = list(map(inspect.getfile, internal)) + [stack[-1][0]]
    internal = [os.path.dirname(fname) for fname in internal]
    stack = stack[:-2]

    name_override = name
    name = None
    desc = None

    for frame in reversed(stack):
        file, _, func, _ = frame
        internals = any(map(file.startswith, internal))
        if internals and func[0] != '_':
            name = func
        desc = frame
        if not internals:
            break

    return name_override or name, desc


def get_callsite(*internal, name=None):
    if hasattr(_callsite, 'current'):
        return _callsite.current
    else:
        return _get_callsite(*internal, name=name)


@contextlib.contextmanager
def set_callsite(*internal, name=None):
    if not hasattr(_callsite, 'current'):
        _callsite.current = get_callsite(*internal, name=name)
        yield
        del _callsite.current
    else:
        yield


def callsite(*internal):
    def decorator(func):
        nonlocal internal
        internal += (func, contextlib.contextmanager)
        @wraps(func)
        def wrapper(*args, **kwargs):
            with set_callsite(*internal, name=func.__name__):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def flatten_dset(root):
    datasets = []
    stack = [root]
    while stack:
        dset = stack.pop()
        datasets.append(dset)
        if isinstance(dset.src, Iterable):
            stack.extend(dset.src)
        elif dset.src is not None:
            stack.append(dset.src)
    return datasets


def group_dsets(root):
    group = []
    groups = [group]
    stack = [root]
    while stack:
        dset = stack.pop()
        group.append(dset)
        src = dset.src
        if isinstance(src, Iterable):
            stack.extend(src)
        elif src is not None:
            if src.sync_required:
                groups.extend(group_dsets(src))
            else:
                stack.append(src)
    return groups
