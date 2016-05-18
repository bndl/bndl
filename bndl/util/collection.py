from itertools import islice, tee, groupby
from collections import Sized


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
