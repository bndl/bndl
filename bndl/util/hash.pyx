from cpython.object cimport PyObject_Hash


cpdef long portable_hash(obj) except -1:
    if obj is None:
        return 0
    else:
        return PyObject_Hash(obj)
