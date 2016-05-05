from itertools import islice, tee, groupby
from collections import Sized
from functools import partial


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


def batch(iterable, size):
    """
        Yield iterables of at most size elements from the given iterable. 
    """

    if isinstance(iterable, Sized):
        for start in range(0, len(iterable), size):
            yield iterable[start:start + size]
    else:
        while True:
            yield list(islice(iterable, size))


def non_empty(iterable):
    '''
    Returns an iterator on the iterable or None if the iterable is empty.
    '''
    if isinstance(iterable, Sized):
        if len(iterable):
            return iter(iterable)
        else:
            return None
    else:
        iterator, any_check = tee(iterable)
        try:
            next(any_check)
            return iterator
        except StopIteration:
            return None


def sortgroupby(iterable, key):
    return groupby(sorted(iterable, key=key), key)
