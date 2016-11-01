from collections import Iterable
from tempfile import mktemp
import inspect
import os.path
import traceback

import graphviz
import contextlib
import threading
from functools import wraps


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

# def dset_to_dot(dset):
#     g = graphviz.Digraph()
#     stack = [dset]
#     while stack:
#         dset = stack.pop()
#         g.node(dset.id, str(dset))
#         if isinstance(dset.src, Iterable):
#             srcs = dset.src
#         elif dset.src is not None:
#             srcs = (dset.src,)
#         else:
#             continue
#
#         stack.extend(srcs)
#         for src in srcs:
#             g.edge(src.id, dset.id)
#     g.render(mktemp(), '.', view=True, cleanup=True)

def dset_to_dot(dset):
    g = graphviz.Digraph()
    g.node(dset.id, dset.name)

    def add_dsets(g, sg, src, dest):
        if isinstance(src, Iterable):
            for src in src:
#                 sg.node(src.id, src.name)
#                 sg.edge(src.id, dest.id)
#                 print(src.name, '->', dest.name)
                add_dsets(g, sg, src, dest)
        elif src is not None:
            print(src.name, '->', dest.name)
            if src.sync_required:
#                 src = src.src
                sg.node(src.id, src.name)
                g.edge(src.id, dest.id, style='dashed')
#             elif src.name != dest.name:
            else:
                sg.node(src.id, src.name)
                g.edge(src.id, dest.id)
#             else:
#                 sg.node(src.id, src.name)
#                 g.edge(src.id, dest.id)
#                 sg = graphviz.Digraph()
#                 g.subgraph(sg)
#                 add_dsets(g, sg, src.src, dest)
    job = group_dsets(dset)
    for group in job:
        sg = g
        for dset in group:
            add_dsets(g, g, dset.src, dset)
    g.render(mktemp(), '.', view=True, cleanup=True)



def dset_expanded_to_dot(dset):
    g = graphviz.Digraph()
    stack = [dset]
    while stack:
        dset = stack.pop()
        g.node(str(dset))
        if isinstance(dset.src, Iterable):
            srcs = dset.src
        elif dset.src is not None:
            srcs = (dset.src,)
        else:
            continue

        stack.extend(srcs)
        for src in srcs:
            g.edge(str(src), str(dset))
    g.render(mktemp(), '.', view=True, cleanup=True)

def parts_to_dot(dset):
    g = graphviz.Digraph()
    g.graph_attr.update(rankdir='LR')
    stack = dset.parts()[:]
    visited = set()
    while stack:
        part = stack.pop()
        if part in visited:
            continue
        else:
            visited.add(part)
        if isinstance(part.src, Iterable):
            srcs = part.src
        elif part.src is not None:
            srcs = (part.src,)
        else:
            continue
        stack.extend(srcs)
        for src in srcs:
            g.edge(str(src), str(part))
    g.render(mktemp(), '.', view=True, cleanup=True)

def tasks_to_dot(tasks):
    g = graphviz.Digraph()
    g.graph_attr.update(rankdir='LR')
    stack = tasks[:]
    tasks = []
    visited = set()
    while stack:
        task = stack.pop()
        if task in visited:
            continue
        else:
            visited.add(task)
        g.node(str(task.part))
        if task.dependencies:
            stack.extend(task.dependencies)
            for src in task.dependencies:
                g.edge(str(src.part), str(task.part))
    g.render(mktemp(), '.', view=True, cleanup=True)
