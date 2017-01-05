import collections
import marshal
import pickle
import sys
import types

from bndl.util.marshalable import marshalable
import cycloudpickle as cloudpickle


def dumps(obj):
    if marshalable(obj):
        try:
            return True, marshal.dumps(obj)
        except ValueError:
            pass
    try:
        return False, pickle.dumps(obj, protocol=4)
    except (pickle.PicklingError, AttributeError):
        return False, cloudpickle.dumps(obj, protocol=4)


def loads(marshalled, msg):
    if marshalled:
        return marshal.loads(msg)
    else:
        return pickle.loads(msg)


# Hook namedtuple, make it picklable
# From https://github.com/apache/spark/blob/d6dc12ef0146ae409834c78737c116050961f350/python/pyspark/serializers.py


_CLS_CACHE = {}


def _restore(name, fields, value):
    """ Restore an object of namedtuple"""
    key = (name, fields)
    cls = _CLS_CACHE.get(key)
    if cls is None:
        cls = collections.namedtuple(name, fields)
        _CLS_CACHE[key] = cls
    return cls(*value)


def _hack_namedtuple(cls):
    """ Make class generated by namedtuple picklable """
    name = cls.__name__
    fields = cls._fields

    def __reduce__(self):
        return (_restore, (name, fields, tuple(self)))
    cls.__reduce__ = __reduce__
    cls._is_namedtuple_ = True
    return cls


def _hijack_namedtuple():
    """ Hack namedtuple() to make it picklable """
    # hijack only one time
    if hasattr(collections.namedtuple, "__hijack"):
        return

    global _old_namedtuple  # or it will put in closure

    def _copy_func(func):
        return types.FunctionType(func.__code__, func.__globals__, func.__name__,
                                  func.__defaults__, func.__closure__)

    _old_namedtuple = _copy_func(collections.namedtuple)

    if sys.version_info < (3, 6):
        def namedtuple(typename, field_names, verbose=False, rename=False):
            cls = _old_namedtuple(typename, field_names, verbose, rename)
            return _hack_namedtuple(cls)
    else:
        def namedtuple(typename, field_names, *, verbose=False, rename=False, module=None):
            cls = _old_namedtuple(typename, field_names, verbose=verbose, rename=rename, module=module)
            return _hack_namedtuple(cls)

    # replace namedtuple with new one
    collections.namedtuple.__globals__["_old_namedtuple"] = _old_namedtuple  # @UndefinedVariable
    collections.namedtuple.__globals__["_hack_namedtuple"] = _hack_namedtuple  # @UndefinedVariable
    collections.namedtuple.__code__ = namedtuple.__code__
    collections.namedtuple.__hijack = 1

    # hack the cls already generated by namedtuple
    # those created in other module can be pickled as normal,
    # so only hack those in __main__ module
    for o in sys.modules["__main__"].__dict__.values():
        if (type(o) is type and o.__base__ is tuple and
                hasattr(o, "_fields") and
                "__reduce__" not in o.__dict__):
            _hack_namedtuple(o)  # hack inplace


_hijack_namedtuple()
