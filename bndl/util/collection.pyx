from collections import defaultdict, Iterable, Sequence, Sized
from itertools import islice, groupby


def batch(iterable, size):
    """
    Yield iterables of at most size elements from the given iterable.
    """
    if isinstance(iterable, Sequence):
        for start in range(0, len(iterable), size):
            yield iterable[start:start + size]
    else:
        while True:
            iterable = iter(iterable)
            l = list(islice(iterable, size))
            if len(l) == 0:
                raise StopIteration()
            else:
                yield l


def split(iterable, key):
    splits = defaultdict(list)
    for e in iterable:
        splits[key(e)].append(e)
    return splits


def sortgroupby(iterable, key):
    return groupby(sorted(iterable, key=key), key)


def is_stable_iterable(obj):
    '''
    Determine if an obj is a stable iterable, excluding string/bytes like
    objects (so may be 'stable collection').
    
    This rule is supposed to catch generators, islices, map objects and the
    lot. They aren't serializable unless materialized in e.g. a list are there
    cases where a) an unserializable type is missed? or b) materializing data
    into a list is a bad (wrong result, waste of resources, etc.)? numpy arrays
    are not wrongly cast to a list through this. That's something ...
    :param obj: The object to test
    '''
    return (
        (isinstance(obj, Iterable) and isinstance(obj, Sized))
    ) and not isinstance(obj, (str, bytes, bytearray))


def ensure_collection(obj):
    return obj if is_stable_iterable(obj) else list(obj)