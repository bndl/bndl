import abc
import atexit
import gzip
import io
import json
import marshal
import os
import pickle
import tempfile

from bndl.execute.worker import current_worker
from bndl.util.exceptions import catch
from bndl.util.funcs import identity


_caches = {}


@atexit.register
def clear_all():
    for cache in _caches.values():
        for dset_cache in cache.values():
            for holder in dset_cache.values():
                holder.clear()
            dset_cache.clear()
        cache.clear()
    _caches.clear()


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

    def _get_cache(self):
        worker = current_worker()
        cache = _caches.setdefault(worker.name, {})
        return cache

    def read(self, part):
        cache = self._get_cache()
        holder = cache[part.dset.id][part.idx]
        try:
            data = holder.read()
        except FileNotFoundError as e:
            raise KeyError(part.idx) from e
        return data

    def write(self, part, data):
        key = current_worker().name, str(part.dset.id), str(part.idx)
        holder = self.holder_cls(key, self)
        holder.write(data)
        cache = self._get_cache()
        cache.setdefault(part.dset.id, {})[part.idx] = holder

    def clear(self, dset_id, part_idx=None):
        cache = self._get_cache()
        if part_idx:
            cache[dset_id][part_idx].clear()
            del cache[dset_id][part_idx]
        else:
            for holder in cache[dset_id].values():
                holder.clear()
            cache[dset_id].clear()
            del cache[dset_id]


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
