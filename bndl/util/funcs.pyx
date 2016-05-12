import inspect


def as_method(o):
    if inspect.ismethod(o):
        return o
    proxy = lambda *args, **kwargs: o(*args, **kwargs)
    proxy.__name__ = o.__name__
    proxy.__doc__ = proxy.__doc__
    return proxy


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
