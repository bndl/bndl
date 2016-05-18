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

def getter(key):
    '''
    Because unpickling itemgetter is for some reason hard to implement ...

        ...
        In [3]: pickle.loads(pickle.dumps(operator.itemgetter(1)))
        ---------------------------------------------------------------------------
        TypeError                                 Traceback (most recent call last)
        <ipython-input-40-2dc39feaf42b> in <module>()
        ----> 1 pickle.loads(pickle.dumps(operator.itemgetter(1)))

        TypeError: itemgetter expected 1 arguments, got 0
    '''
    return partial(_getter, key)


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
