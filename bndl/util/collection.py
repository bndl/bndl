from itertools import islice, groupby
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


def sortgroupby(iterable, key):
    return groupby(sorted(iterable, key=key), key)
