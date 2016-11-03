cdef class _Framer:
    cdef object file_write
    cdef object current_frame

cdef class Pickler:
    cdef int protocol
    cdef int proto
    cdef bint bin
    cdef bint fast
    cdef bint fix_imports

    cdef object _file_write
    cdef _Framer framer
    cdef object write
    cdef dict memo
