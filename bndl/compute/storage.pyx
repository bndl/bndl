from os.path import getsize
import abc
import asyncio
import contextlib
import gzip
import io
import json
import logging
import marshal
import os
import pickle
import struct
import tempfile

from bndl.net.sendfile import file_attachment, sendfile
from bndl.net.serialize import attach, attachment
from bndl.rmi.blocks import Block
from bndl.util import aio
from bndl.util.exceptions import catch
from bndl.util.funcs import identity


logger = logging.getLogger(__name__)


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



class BytearrayIO(io.RawIOBase):
    def __init__(self, buffer, mode='rb'):
        self.buffer = buffer
        self.mode = mode
        self.pos = 0

    def read(self, size=-1):
        # TODO investigate use of memoryview
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


class StorageContainerFactory(object):
    serialize = None
    deserialize = None
    mode = None
    io_wrapper = None
    container_cls = None

    def __init__(self, location, serialization='pickle', compression=None):
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
            elif serialization == 'msgpack':
                import msgpack
                self.serialize = msgpack.dump
                self.deserialize = msgpack.load
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
                self.container_cls = SerializedInMemory
            else:
                self.container_cls = InMemory
        elif location == 'disk':
            if serialization is None:
                raise ValueError('can\'t specify location without specifying serialization')
            self.container_cls = OnDisk
        elif isinstance(location, type):
            self.container_cls = location
        else:
            raise ValueError('location must be "memory" or "disk" or a class which conforms to'
                             ' bndl.compute.storage.Container')

    def __call__(self, container_id):
        return self.container_cls(container_id, self)


class Container(object):
    def __init__(self, container_id, provider):
        self.id = container_id
        self.provider = provider

    @abc.abstractmethod
    def read(self):
        ...

    @abc.abstractmethod
    def write(self, data):
        ...


class InMemory(Container):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = None

    def read(self):
        return self.data

    def write(self, data):
        self.data = data

    def clear(self):
        self.data = None


class SerializedContainer(Container):
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


class SerializedInMemory(SerializedContainer, InMemory, Block):
    def _open(self, mode):
        assert mode in 'rw'
        if mode[0] == 'w':
            self.data = bytearray()
            baio = BytearrayIO(self.data, mode)
        else:
            # TODO use BytearrayIO, this costs a memory copy
            baio = io.BytesIO(self.data)
            baio.mode = mode
        return baio


    @property
    def size(self):
        return len(self.data) if self.data else 0


    def to_disk(self):
        self.__class__ = OnDisk
        self.__init__(self.id, self.provider)
        fileobj = self.open('w')
        try:
            fileobj.write(self.data)
        finally:
            with catch():
                fileobj.close()
        del self.data



def gettempdir():
    tempdir = tempfile.gettempdir()
    if os.path.exists('/proc/mounts'):
        with open('/proc/mounts') as mounts:
            for mount in mounts:
                mount = mount.split()
                if mount[1] == tempdir:
                    if mount[0] == 'tmpfs' and os.path.isdir('/var/tmp'):
                        return '/var/tmp'
                    break
    return tempdir



class OnDisk(SerializedContainer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        * dirpath, filename = self.id
        dirpath = os.path.join(gettempdir(), 'bndl', *map(str, dirpath))
        os.makedirs(dirpath, exist_ok=True)
        self.filepath = os.path.join(dirpath, str(filename))


    def _open(self, mode):
        return open(self.filepath, mode + 'b')


    def clear(self):
        try:
            os.remove(self.filepath)
        except FileNotFoundError:
            pass
        except Exception:
            logger.exception('Unable to clear file %s for id %s' %
                             (self.filepath, self.id))

    @property
    def size(self):
        return getsize(self.filepath)


    def __getstate__(self):
        attach(*file_attachment(self.filepath, 0, os.path.getsize(self.filepath)))
        return dict(self.__dict__)


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.data = attachment(self.filepath.encode('utf-8'))
        self.__class__ = SerializedInMemory


    def __del__(self):
        with catch():
            os.remove(self.filepath)
