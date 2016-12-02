from functools import partial, wraps
import inspect

from cytoolz import pluck
from cytoolz.utils import no_default


def as_method(o):
    if inspect.ismethod(o):
        return o
    else:
        @wraps(o)
        def proxy(*args, **kwargs):
            return o(*args, **kwargs)
        return proxy


class ppluck(object):
    def __init__(self, ind, seqs, default=no_default):
        self.ind = ind
        self.seqs = seqs
        self.default = default

    def __iter__(self):
        return pluck(self.ind, self.seqs, self.default)

    def __reduce__(self):
        return identity, (tuple(iter(self)),)


def partial_func(func, *args, **kwargs):
    if args:
        if kwargs:
            return partial(func, *args, **kwargs)
        else:
            return partial(func, *args)
    elif kwargs:
        return partial(func, **kwargs)
    else:
        return func


def _getter(key, obj):
    return obj[key]


def _getter_list(index, obj):
    return tuple(obj[i] for i in index)


def getter(index):
    '''
    Because unpickling itemgetter is for some reason hard to implement ...

        ...
        In [3]: pickle.loads(pickle.dumps(operator.itemgetter(1)))
        ---------------------------------------------------------------------------
        TypeError                                 Traceback (most recent call last)
        <ipython-input-40-2dc39feaf42b> in <module>()
        ----> 1 pickle.loads(pickle.dumps(operator.itemgetter(1)))

        TypeError: itemgetter expected 1 arguments, got 0


    This also hurts cytoolz:
        An unknown exception occurred in connection localdomain.localhost.driver.9844.0
        Traceback (most recent call last):
          File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/net/peer.py", line 279, in _serve
            msg = yield from self.recv()
          File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/net/peer.py", line 75, in recv
            msg = yield from self.conn.recv(timeout)
          File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/net/connection.py", line 208, in recv
            return serialize.load(*payload)
          File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/net/serialize.py", line 61, in load
            value = serialize.loads(marshalled, msg)
          File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/util/serialize.py", line 27, in loads
            return pickle.loads(msg)
          File "cytoolz/itertoolz.pyx", line 1174, in cytoolz.itertoolz._getter_list.__cinit__ (cytoolz/itertoolz.c:15892)
        TypeError: __cinit__() takes exactly 1 positional argument (0 given)
     '''
    if isinstance(index, list):
        return partial(_getter_list, index)
    else:
        return partial(_getter, index)


def key_or_getter(key):
    if key is not None and not callable(key):
        return getter(key)
    else:
        return key


def prefetch(function, sequence):
    '''
    Utility similar to map but which is one invocation ahead of the results returned.
    Useful in combination with functions which set of work and return a Future.
    :param function: the function to apply.
    :param sequence: a sequence of arguments to apply the function to.
    '''
    sequence = iter(sequence)
    pending = function(next(sequence))
    for args in sequence:
        nxt = function(args)
        yield pending
        pending = nxt
    yield pending


def star_prefetch(function, sequence):
    '''
    Like prefetch, but applies the elements of the sequence as star args.
    :param function: the function to apply.
    :param sequence: a sequence of arguments to apply the function to.
    '''
    return prefetch(lambda args: function(*args), sequence)


def neg(x):
    return -x

def noop(*args, **kwargs):
    pass

# functions from https://github.com/pytoolz/toolz/blob/master/toolz/tests/test_itertoolz.py

def identity(x):
    return x

def iseven(x):
    return x % 2 == 0

def isodd(x):
    return x % 2 == 1

def inc(x):
    return x + 1

def double(x):
    return 2 * x
