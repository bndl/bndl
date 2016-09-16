import inspect
from functools import partial


def as_method(o):
    if inspect.ismethod(o):
        return o
    proxy = lambda *args, **kwargs: o(*args, **kwargs)
    proxy.__name__ = o.__name__
    proxy.__doc__ = proxy.__doc__
    return proxy


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


def neg(x):
    return -x

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
