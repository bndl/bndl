from itertools import islice


cpdef bint marshalable(object obj):
    if obj is None:
        return True

    t = type(obj)

    if t in (bool, int, float, complex, str, bytes, bytearray):
        return True

    elif t in (tuple, list, set, frozenset):
        for e in islice(obj, 100):
            if not marshalable(e):
                return False
        return True

    elif t == dict:
        for k, v in islice(obj.items(), 100):
            if not marshalable(k) or not marshalable(v):
                return False
        return True

    else:
        return False