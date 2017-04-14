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

from functools import partial
from itertools import chain
from os.path import getsize
import atexit
import contextlib
import importlib
import io
import json
import logging
import marshal
import mmap
import os
import pickle
import shutil
import struct
import tempfile

from cytoolz import compose
import psutil

from bndl.net.sendfile import file_attachment, is_remote
from bndl.net.serialize import attach, attachment
from bndl.util.compat import lz4_compress, lz4_decompress
from bndl.util.conf import String
from bndl.util.funcs import noop
from bndl.util.strings import decode, random_id
import bndl


logger = logging.getLogger(__name__)


def _is_writable_path(path):
    try:
        with tempfile.NamedTemporaryFile(dir=path):
            pass
    except:
        return False
    else:
        return True


def _is_disk_path(path):
    for p in psutil.disk_partitions(True):
        if path.startswith(p.mountpoint) and p.fstype == 'tmpfs':
            return False
    return True


def get_workdir(disk, pid=None):
    paths = [
        os.environ.get('TMPDIR'),
        os.environ.get('TEMP'),
        os.environ.get('TMP'),
    ]

    paths.append(tempfile.gettempdir())

    if disk:
        paths.append('/var/tmp')
        paths.append('/tmp')
    else:
        paths.append('/run/user/' + str(os.getuid()))
        paths.append('/run/shm')
        paths.append('/dev/shm')
        paths.append('/tmp')

    paths = [
        path for path in paths
        if path and
           os.path.exists(path) and
           _is_disk_path(path) == disk and
           _is_writable_path(path)
    ]

    path = os.path.join(paths[0], 'bndl', str(pid or os.getpid()))
    os.makedirs(path, exist_ok=True)

    return path


@atexit.register
def _clean_workdirs():
    paths = (
        bndl.conf['bndl.compute.storage.workdir_mem'],
        bndl.conf['bndl.compute.storage.workdir_disk'],
    )

    for path in paths:
        try:
            shutil.rmtree(path)
        except (FileNotFoundError, OSError):
            pass



workdir_mem = String(get_workdir(False), desc='The ram backed directory for bndl.compute (used for caching, '
                                              'shuffle data, etc.).')
workdir_disk = String(get_workdir(True), desc='The disk backed working directory for bndl.compute (used for '
                                              'caching, shuffle data, etc.).')


LENGTH_FIELD_FMT = 'I'
LENGTH_FIELD_SIZE = struct.calcsize(LENGTH_FIELD_FMT)



def text_dumps(lines):
    assert not isinstance(lines, str)
    chunks = (line.encode() for line in lines)
    return binary_dumps(chunks)


def text_loads(data):
    chunks = binary_load_gen(data)
    return [chunk.decode() for chunk in chunks]


def binary_dumps(chunks):
    assert not isinstance(chunks, bytes)
    len_fmt = LENGTH_FIELD_FMT
    pack = struct.pack
    return b''.join(chain.from_iterable(
        (pack(len_fmt, len(chunk)), chunk) for chunk in chunks
    ))


def binary_loads(data):
    return list(binary_load_gen(data))


def binary_load_gen(data):
    len_fmt = LENGTH_FIELD_FMT
    unpack = struct.unpack
    data_len = len(data)
    pos = 0
    while pos < data_len:
        chunk_len = unpack(len_fmt, data[pos:pos + LENGTH_FIELD_SIZE])[0]
        pos += LENGTH_FIELD_SIZE
        yield data[pos:pos + chunk_len]
        pos += chunk_len



class ByteArrayIO(io.RawIOBase):
    def __init__(self, buffer, mode='rb'):
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
        return b

    def write(self, b):
        self.buffer.extend(b)

    def readable(self):
        return 'r' in self.mode or '+' in self.mode

    def writable(self):
        return 'w' in self.mode or 'a' in self.mode or '+' in self.mode

    def seekable(self):
        return True

    @property
    def closed(self):
        return False

    def tell(self):
        return self.pos

    def seek(self, pos, whence=io.SEEK_SET):
        if whence == io.SEEK_CUR:
            pos += self.pos
        elif whence == io.SEEK_END:
            pos += len(self.buffer)
        if pos < 0:
            pos = 0
        assert pos < len(self.buffer)
        self.pos = pos
        return pos



class InMemoryData(object):
    '''
    Helper class for exchanging binary data between peers. It uses the attach and
    attachment utilities from bndl.net.serialize to optimize sending the already
    serialized data.
    '''

    def __init__(self, data_id, data=None):
        self.id = data_id
        self.data = bytearray(data) if data is not None else bytearray()


    @property
    def size(self):
        return len(self.data)


    def open(self, mode):
        assert 'b' in mode
        return ByteArrayIO(self.data, mode)


    def view(self):
        with open(self.filepath, 'rb') as f:
            return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


    def __getstate__(self):
        data = self.data
        @contextlib.contextmanager
        def _attacher(loop, writer):
            yield len(data), partial(writer.write, data)
        attach(str(self.id).encode(), _attacher)
        state = dict(self.__dict__)
        state.pop('data', None)
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.data = attachment(str(self.id).encode())


    def remove(self):
        self.data = bytearray()


    def to_disk(self):
        file = FileData(self.id, len(self.data or ()))
        remove_old = file.remove
        file.remove = noop
        with file.open('wb') as f:
            f.write(self.data)
        self.__dict__.update(file.__dict__)
        self.remove = remove_old
        self.__class__ = FileData
        self.__dict__.pop('data', None)



class FileData(object):
    @property
    def workdir(self):
        return bndl.conf['bndl.compute.storage.workdir_disk']

    def __init__(self, file_id, allocate=0):
        self.id = file_id
        * dirpath, filename = self.id
        dirpath = os.path.join(self.workdir, *map(str, dirpath))
        os.makedirs(dirpath, exist_ok=True)
        self.filepath = os.path.join(dirpath, str(filename))
        if allocate:
            with open(self.filepath, 'wb') as f:
                os.posix_fallocate(f.fileno(), 0, allocate)


    @property
    def size(self):
        try:
            return getsize(self.filepath)
        except FileNotFoundError:
            return 0


    def open(self, mode):
        return open(self.filepath, mode)


    def view(self):
        with open(self.filepath, 'rb') as f:
            return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


    def __getstate__(self):
        filepath = self.filepath
        workdir = self.workdir
        attach(*file_attachment(filepath, 0, os.path.getsize(filepath)))
        return {
            'id': self.id,
            'filepath': (workdir, filepath.replace(workdir, '')),
        }


    def __setstate__(self, state):
        workdir_src, filepath_src = state.pop('filepath')
        data = attachment((workdir_src + filepath_src).encode('utf-8'))

        self.__dict__.update(state)

        if is_remote(data):
            self.data = memoryview(data)[1:]
            self.__class__ = InMemoryData
        else:
            filepath_dst = self.workdir + filepath_src
            filepath_src = workdir_src + filepath_src
            if filepath_src == filepath_dst:
                filepath_dst = filepath_dst + '.' + random_id()
            os.makedirs(os.path.dirname(filepath_dst), exist_ok=True)
            os.link(filepath_src, filepath_dst)
            self.filepath = filepath_dst


    def remove(self):
        if hasattr(self, 'filepath'):
            try:
                os.unlink(self.filepath)
            except (AttributeError, FileNotFoundError):
                pass
            except Exception:
                logger.exception('Unable to clear file %s for id %s' %
                                 (self.filepath, self.id))


    def __del__(self):
        self.remove()


class SharedMemoryData(FileData):
    @property
    def workdir(self):
        return bndl.conf['bndl.compute.storage.workdir_mem']

    def to_disk(self):
        workdir_mem = bndl.conf['bndl.compute.storage.workdir_mem']
        workdir_disk = bndl.conf['bndl.compute.storage.workdir_disk']
        filepath_src = self.filepath
        if filepath_src.startswith(workdir_mem):
            filepath_dst = filepath_src.replace(workdir_mem, workdir_disk)
            os.makedirs(os.path.dirname(filepath_dst), exist_ok=True)
            shutil.copyfile(filepath_src, filepath_dst)
            self.filepath = filepath_dst
            os.unlink(filepath_src)
        self.__class__ = FileData




class ContainerFactory(object):
    def __init__(self, location, serialization='pickle', compression=None):
        self.location = location
        self.serialization = serialization
        self.compression = compression

        if serialization == None:
            self.serialize, self.deserialize = None, None
        else:
            if serialization == 'json':
                self.serialize = compose(str.encode, json.dumps)
                self.deserialize = compose(json.loads, decode)
            elif serialization == 'marshal':
                self.serialize = marshal.dumps
                self.deserialize = marshal.loads
            elif serialization == 'pickle':
                self.serialize = pickle.dumps
                self.deserialize = pickle.loads
            elif serialization == 'msgpack':
                try:
                    import msgpack
                except ImportError:
                    from pandas import msgpack
                self.serialize = msgpack.dumps
                self.deserialize = msgpack.loads
            elif serialization == 'text':
                self.serialize = text_dumps
                self.deserialize = text_loads
            elif serialization == 'binary':
                self.serialize = binary_dumps
                self.deserialize = binary_loads
            elif isinstance(serialization, (list, tuple)) \
                and len(serialization) != 3 \
                and all(callable, serialization[:2]):
                self.serialize, self.deserialize = serialization
            else:
                raise ValueError('Serialization %r is unsupported')

        if location == 'memory':
            if self.serialize:
                self.container_cls = SharedMemoryContainer  # SerializedInMemoryContainer
            else:
                self.container_cls = InMemoryContainer
        elif location == 'disk':
            if serialization is None:
                serialization = 'pickle'
            self.container_cls = OnDiskContainer
        elif isinstance(location, type):
            self.container_cls = location
        else:
            raise ValueError('location must be "memory" or "disk" or a class which conforms to'
                             ' bndl.compute.storage.Container')

        if compression is not None:
            if compression == 'lz4':
                compress = (lz4_compress,)
                decompress = (lz4_decompress, bytes)
            elif isinstance(compression, str):
                mod = importlib.import_module(compression)
                compress = (mod.compress,)
                decompress = (mod.decompress,)
            elif isinstance(compression, tuple) and all(map(callable, compression)):
                compress = compression[:1]
                decompress = compression[1:]
            else:
                raise ValueError('compression must be None, a module name which provides the'
                                 ' compress and decompress functions (like "gzip" or "lz4" )or a'
                                 ' 2-tuple of callables to provide (transparant like dumps/loads)'
                                 ' (de)compression on a bytes-like object, not %r' % compression)

            compress = compress + (self.serialize,)
            decompress = (self.deserialize,) + decompress

            self.serialize = compose(*compress)
            self.deserialize = compose(*decompress)

            if serialization is None:
                raise ValueError('can\'t specify compression without specifying serialization')


    def __call__(self, container_id):
        return self.container_cls(container_id, self)

    def __repr__(self):
        return '<ContainerFactory location=%s, serialization=%s, compression=%s>' % (
            self.location, self.serialization, self.compression)



class Container(object):
    def __init__(self, container_id, provider):
        self.id = container_id
        self.provider = provider

    def read(self):
        return next(self.read_all())

    def write(self, data):
        self.write_all((data,))

    def read_all(self):
        raise NotImplemented()

    def write_all(self):
        raise NotImplemented()

    def clear(self):
        raise NotImplemented()

    def __del__(self):
        self.clear()



class InMemoryContainer(Container):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = ()


    def read_all(self):
        return iter(self.data)


    def write_all(self, chunks):
        self.data = list(chunks)

    _read_all = read_all
    _write_all = write_all


    def clear(self):
        self.data = ()



class SerializedContainer(Container):
    def read_all(self):
        yield from map(self.provider.deserialize, self._read_all())

    def write_all(self, chunks):
        chunks = map(self.provider.serialize, chunks)
        self._write_all(chunks)



class SerializedInMemoryContainer(SerializedContainer, InMemoryContainer):
    @property
    def size(self):
        return sum(map(len, self.data))


    def __getstate__(self):
        state = dict(self.__dict__)
        state['data'] = [InMemoryData((self.id, idx), chunk)
                         for idx, chunk in enumerate(self.data)]
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.data = [block.data for block in state['data']]


    def to_disk(self):
        on_disk = OnDiskContainer(self.id, self.provider)
        clear_old = on_disk.clear
        on_disk.clear = noop
        on_disk._write_all(self.data)
        self.__dict__.update(on_disk.__dict__)
        self.clear = clear_old
        self.__class__ = OnDiskContainer
        self.__dict__.pop('data')



class OnDiskContainer(SerializedContainer):
    Data = FileData

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.Data(self.id)


    @property
    def size(self):
        return self.file.size


    def _read_all(self):
        with self.file.open('rb') as f:
            len_fmt = LENGTH_FIELD_FMT
            unpack = struct.unpack
            read = f.read
            while True:
                y = read(LENGTH_FIELD_SIZE)
                if not y:
                    break
                chunk_len = unpack(len_fmt, y)[0]
                yield read(chunk_len)


    def _write_all(self, chunks):
        len_fmt = LENGTH_FIELD_FMT
        pack = struct.pack
        with self.file.open('wb') as f:
            for chunk in chunks:
                f.writelines((pack(len_fmt, len(chunk)), chunk))


    def clear(self):
        try:
            os.remove(self.filepath)
        except (AttributeError, FileNotFoundError):
            pass
        except Exception:
            logger.exception('Unable to clear file %s for id %s' %
                             (self.filepath, self.id))



class SharedMemoryContainer(OnDiskContainer):
    Data = SharedMemoryData

    def to_disk(self):
        self.file.to_disk()
