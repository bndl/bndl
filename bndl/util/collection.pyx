# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict, Iterable, Sequence, Sized
from itertools import islice, groupby, chain


def seqlen(seq):
    '''
    Get length of a sequence (which is compatible with i.a. scipy.sparse matrices)
    '''
    try:
        return len(seq)
    except:
        if hasattr(seq, 'shape'):
            return seq.shape[0]
        else:
            raise


def issequence(o):
    return hasattr(o, '__len__') and hasattr(o, '__getitem__')


def batch(iterable, size):
    """
    Yield iterables of at most size elements from the given iterable.
    """
    if issequence(iterable):
        for start in range(0, seqlen(iterable), size):
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


def flatten(item):
    if isinstance(item, Iterable) and not isinstance(item, (str, bytes, bytearray)):
        for i in item:
            yield from flatten(i)
    else:
        yield item


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
