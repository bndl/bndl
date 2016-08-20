import abc
import atexit
import gzip
import io
import json
import logging
import marshal
import os
import pickle
import struct
import tempfile

from bndl.execute.worker import current_worker
from bndl.util.exceptions import catch
from bndl.util.funcs import identity


logger = logging.getLogger(__name__)


_caches = {}


@atexit.register
def clear_all():
    for cache in _caches.values():
        for holder in cache.values():
            holder.clear()
        cache.clear()
    _caches.clear()


_LENGTH_FIELD_FMT = 'I'
_LENGTH_FIELD_SIZE = struct.calcsize(_LENGTH_FIELD_FMT)


def _text_dump(lines, fileobj):
    chunks = (line.encode() for line in lines)
    _binary_dump(chunks, fileobj)

def _text_load(fileobj):
    chunks = _binary_load_gen(fileobj)
    return [chunk.decode() for chunk in chunks]

def _binary_dump(chunks, fileobj):
    len_fmt = _LENGTH_FIELD_FMT
    pack = struct.pack
    write = fileobj.write
    for chunk in chunks:
        write(pack(len_fmt, len(chunk)))
        write(chunk)

def _binary_load(fileobj):
    return list(_binary_load_gen(fileobj))

def _binary_load_gen(fileobj):
    len_fmt = _LENGTH_FIELD_FMT
    len_buffer = bytearray(_LENGTH_FIELD_SIZE)
    read = fileobj.read
    readinto = fileobj.readinto
    unpack = struct.unpack
    while True:
        if not readinto(len_buffer):
            break
        chunk_len = unpack(len_fmt, len_buffer)[0]
        yield read(chunk_len)


class _GzipIOWrapper(gzip.GzipFile):
    def __init__(self, fileobj):
        super().__init__(fileobj=fileobj)


class CacheProvider(object):
    serialize = None
    deserialize = None
    mode = None
    io_wrapper = None
    holder_cls = None

    def __init__(self, location, serialization, compression):

        if serialization == None:
            self.serialize, self.deserialize = None, None
        else:
            if serialization == 'json':
                self.serialize = json.dump
                self.deserialize = json.load
                self.mode = 't'
            elif serialization == 'marshal':
                self.serialize = marshal.dump
                self.deserialize = marshal.load
                self.mode = 'b'
            elif serialization == 'pickle':
                self.serialize = pickle.dump
                self.deserialize = pickle.load
                self.mode = 'b'
            elif serialization == 'text':
                self.serialize = _text_dump
                self.deserialize = _text_load
                self.mode = 'b'
            elif serialization == 'binary':
                self.serialize = _binary_dump
                self.deserialize = _binary_load
                self.mode = 'b'
            elif isinstance(serialization, (list, tuple)) \
                and len(serialization) != 3 \
                and all(callable, serialization):
                self.serialize, self.deserialize, self.mode = serialization
            else:
                raise ValueError('serialization must one of json, marshal, pickle or a 3-tuple of'
                                 ' dump(data, fileobj) and load(fileobj) functions and "b" or "t" '
                                 ' (indicating binary or text mode), not %r'
                                 % serialization)

        if compression is None:
            self.io_wrapper = identity
        else:
            if compression == 'gzip':
                self.io_wrapper = _GzipIOWrapper
                if serialization is None:
                    raise ValueError('can\'t specify compression without specifying serialization')
            elif not callable(compression):
                raise ValueError('compression must be None, "gzip" or a callable to provide'
                                 ' (transparant) (de)compression on a file-like object,'
                                 ' not %r' % compression)
            else:
                self.io_wrapper = compression


        if location == 'memory':
            if self.serialize:
                self.holder_cls = SerializedInMemory
            else:
                self.holder_cls = InMemory
        elif location == 'disk':
            if serialization is None:
                raise ValueError('can\'t specify location without specifying serialization')
            self.holder_cls = OnDisk
        elif isinstance(location, type):
            self.holder_cls = location
        else:
            raise ValueError('location must be "memory" or "disk" or a class which conforms to'
                             ' bndl.compute.cache.Holder')

    def read(self, part):
        holder = _caches[part.dset.id][part.idx]
        try:
            data = holder.read()
        except FileNotFoundError as e:
            raise KeyError(part.idx) from e
        return data

    def write(self, part, data):
        key = str(part.dset.id), str(part.idx)
        holder = self.holder_cls(key, self)
        holder.write(data)
        _caches.setdefault(part.dset.id, {})[part.idx] = holder

    def clear(self, dset_id, part_idx=None):
        if part_idx:
            _caches[dset_id][part_idx].clear()
            del _caches[dset_id][part_idx]
        else:
            for holder in _caches[dset_id].values():
                holder.clear()
            _caches[dset_id].clear()
            del _caches[dset_id]


class Holder(object):
    def __init__(self, key, provider):
        self.key = key
        self.provider = provider

    @abc.abstractmethod
    def read(self):
        ...

    @abc.abstractmethod
    def write(self, data):
        ...


class InMemory(Holder):
    data = None

    def read(self):
        return self.data

    def write(self, data):
        self.data = data

    def clear(self):
        self.data = None


class SerializedHolder(Holder):
    def read(self):
        fileobj = self.open('r')
        try:
            return self.provider.deserialize(fileobj)
        finally:
            with catch():
                fileobj.close()

    def write(self, data):
        fileobj = self.open('w')
        try:
            return self.provider.serialize(data, fileobj)
        finally:
            with catch():
                fileobj.close()

    def open(self, mode):
        fileobj = self._open(mode)
        fileobj = self.provider.io_wrapper(fileobj)
        if self.provider.mode == 't':
            fileobj = io.TextIOWrapper(fileobj)
        return fileobj



class BytearrayIO(io.RawIOBase):
    def __init__(self, buffer, mode):
        self.buffer = buffer
        self.mode = mode
        self.pos = 0

    def read(self, size=-1):
        if size == -1 or not size:
            b = self.buffer[self.pos:]
            self.pos = len(self.buffer)
        else:
            b = self.buffer[self.pos:self.pos + size]
            self.pos += size
        return bytes(b)

    def write(self, b):
        self.buffer.extend(b)

    def readable(self):
        return True

    def writable(self):
        return True


class SerializedInMemory(SerializedHolder):
    data = None

    def _open(self, mode):
        assert mode in 'rw'
        if mode[0] == 'w':
            self.data = bytearray()
            baio = BytearrayIO(self.data, mode)
        else:
            baio = io.BytesIO(self.data)
            baio.mode = mode
        return baio

    def clear(self):
        self.data = None


class OnDisk(SerializedHolder):
    def _open(self, mode):
        *dirpath, filename = self.key
        dirpath = os.path.join(tempfile.gettempdir(), *dirpath)
        filepath = os.path.join(dirpath, filename)
        os.makedirs(dirpath, exist_ok=True)
        return open(filepath, mode + 'b')

    def clear(self):
        try:
            os.remove(os.path.join(tempfile.gettempdir(), *self.key))
        except Exception:
            logger.exception('Unable to clear cache file %s for cache key %s' %
                             (self._get_path(), self.key))
