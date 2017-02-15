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

from itertools import chain
from os.path import getsize
import atexit
import importlib
import json
import logging
import marshal
import os
import pickle
import shutil
import struct
import tempfile

from cytoolz.functoolz import compose

from bndl.compute.blocks import Block
from bndl.net.sendfile import file_attachment, is_remote
from bndl.net.serialize import attach, attachment
from bndl.util.conf import String
from bndl.util.funcs import noop
import bndl
import lz4


logger = logging.getLogger(__name__)


_LENGTH_FIELD_FMT = 'I'
_LENGTH_FIELD_SIZE = struct.calcsize(_LENGTH_FIELD_FMT)


work_dir = String(None, desc='The working directory for bndl.compute (used for caching, shuffle '
                             'data, etc.).')


def _text_dumps(lines):
    chunks = (line.encode() for line in lines)
    return _binary_dumps(chunks)


def _text_loads(data):
    chunks = _binary_load_gen(data)
    return [chunk.decode() for chunk in chunks]


def _binary_dumps(chunks):
    len_fmt = _LENGTH_FIELD_FMT
    pack = struct.pack
    return b''.join(chain.from_iterable(
        (pack(len_fmt, len(chunk)), chunk) for chunk in chunks
    ))


def _binary_loads(data):
    return list(_binary_load_gen(data))


def _binary_load_gen(data):
    len_fmt = _LENGTH_FIELD_FMT
    unpack = struct.unpack
    data_len = len(data)
    pos = 0
    while pos < data_len:
        chunk_len = unpack(len_fmt, data[pos:pos + _LENGTH_FIELD_SIZE])[0]
        pos += _LENGTH_FIELD_SIZE
        yield data[pos:pos + chunk_len]
        pos += chunk_len



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
                self.serialize = json.dumps
                self.deserialize = json.loads
                self.mode = 't'
            elif serialization == 'marshal':
                self.serialize = marshal.dumps
                self.deserialize = marshal.loads
                self.mode = 'b'
            elif serialization == 'pickle':
                self.serialize = pickle.dumps
                self.deserialize = pickle.loads
                self.mode = 'b'
            elif serialization == 'msgpack':
                try:
                    import msgpack
                except ImportError:
                    from pandas import msgpack
                self.serialize = msgpack.dumps
                self.deserialize = msgpack.loads
                self.mode = 'b'
            elif serialization == 'text':
                self.serialize = _text_dumps
                self.deserialize = _text_loads
                self.mode = 'b'
            elif serialization == 'binary':
                self.serialize = _binary_dumps
                self.deserialize = _binary_loads
                self.mode = 'b'
            elif isinstance(serialization, (list, tuple)) \
                and len(serialization) != 3 \
                and all(callable, serialization[:2]):
                self.serialize, self.deserialize, self.mode = serialization
            else:
                raise ValueError('serialization must one of json, marshal, pickle or a 3-tuple of'
                                 ' dumps(data) and loads(data) functions and "b" or "t" '
                                 ' (indicating binary or text mode), not %r'
                                 % (serialization,))

        if location == 'memory':
            if self.serialize:
                self.container_cls = SerializedInMemory
            else:
                self.container_cls = InMemory
        elif location == 'disk':
            if serialization is None:
                serialization = 'pickle'
            self.container_cls = OnDisk
        elif isinstance(location, type):
            self.container_cls = location
        else:
            raise ValueError('location must be "memory" or "disk" or a class which conforms to'
                             ' bndl.compute.storage.Container')

        if compression is not None:
            if compression == 'lz4':
                self.serialize = compose(lz4.compress, self.serialize)
                self.deserialize = compose(self.deserialize, lz4.decompress, bytes)
            elif isinstance(compression, str):
                mod = importlib.import_module(compression)
                self.serialize = compose(mod.compress, self.serialize)
                self.deserialize = compose(self.deserialize, mod.decompress)
            elif isinstance(compression, tuple) and all(map(callable, compression)):
                compress, decompress = compression
                self.serialize = compose(compress, self.serialize)
                self.deserialize = compose(self.deserialize, decompress)
            else:
                raise ValueError('compression must be None, a module name which provides the'
                                 ' compress and decompress functions (like "gzip" or "lz4" )or a'
                                 ' 2-tuple of callables to provide (transparant like dumps/loads)'
                                 ' (de)compression on a bytes-like object, not %r' % compression)

            if serialization is None:
                raise ValueError('can\'t specify compression without specifying serialization')


    def __call__(self, container_id):
        return self.container_cls(container_id, self)


class Container(object):
    def __init__(self, container_id, provider):
        self.id = container_id
        self.provider = provider

    def read(self):
        raise NotImplemented()

    def write(self, data):
        raise NotImplemented()

    def clear(self):
        raise NotImplemented()

    def __del__(self):
        self.clear()


class InMemory(Container):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = None

    def read(self):
        return self.data

    def write(self, data):
        self.data = data

    _read = read
    _write = write

    def clear(self):
        self.data = None


class SerializedContainer(Container):
    def read(self):
        return self.provider.deserialize(self._read())

    def write(self, data):
        self._write(self.provider.serialize(data))


class SerializedInMemory(SerializedContainer, InMemory, Block):
    @property
    def size(self):
        return len(self.data) if self.data else 0

    def to_disk(self):
        on_disk = OnDisk(self.id, self.provider)
        clear_old = on_disk.clear
        on_disk.clear = noop
        on_disk._write(self.data)
        self.__dict__.update(on_disk.__dict__)
        self.clear = clear_old
        self.__class__ = OnDisk
        self.__dict__.pop('data')



def _get_work_dir():
    work_dir = os.environ.get('TMPDIR') or \
               os.environ.get('TEMP') or \
               os.environ.get('TMP') or \
               bndl.conf.get('bndl.compute.storage.work_dir') or \
               tempfile.gettempdir()
    if os.path.exists('/proc/mounts'):
        with open('/proc/mounts') as mounts:
            for mount in mounts:
                mount = mount.split()
                if work_dir.startswith(mount[1]) and mount[0] == 'tmpfs':
                    if os.path.exists('/var/tmp'):
                        work_dir = '/var/tmp'
    return tempfile.mkdtemp('', 'bndl-%s-' % str(os.getpid()), work_dir)


_work_dir = None

def get_work_dir():
    global _work_dir
    if _work_dir is None:
        _work_dir = _get_work_dir()
    return _work_dir


@atexit.register
def clean_work_dir():
    if _work_dir:
        try:
            shutil.rmtree(_work_dir)
        except (FileNotFoundError, OSError):
            pass



class OnDisk(SerializedContainer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        * dirpath, filename = self.id
        dirpath = os.path.join(get_work_dir(), *map(str, dirpath))
        os.makedirs(dirpath, exist_ok=True)
        self.filepath = os.path.join(dirpath, str(filename))


    def _read(self):
        with open(self.filepath, 'r' + self.provider.mode) as f:
            return f.read()


    def _write(self, data):
        with open(self.filepath, 'w' + self.provider.mode) as f:
            f.write(data)


    def clear(self):
        try:
            os.remove(self.filepath)
        except (AttributeError, FileNotFoundError):
            pass
        except Exception:
            logger.exception('Unable to clear file %s for id %s' %
                             (self.filepath, self.id))


    @property
    def size(self):
        return getsize(self.filepath)


    def __getstate__(self):
        attach(*file_attachment(self.filepath, 0, os.path.getsize(self.filepath)))
        return {
            'id': self.id,
            'filepath': self.filepath,
            'provider': self.provider,
        }


    def __setstate__(self, state):
        self.__dict__.update(state)
        data = attachment(self.filepath.encode('utf-8'))
        if is_remote(data):
            self.data = memoryview(data)[1:]
            self.__class__ = SerializedInMemory
