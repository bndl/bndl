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

from itertools import product
from unittest.case import TestCase
import asyncio
import random
import string

from bndl.compute.storage import ContainerFactory
from bndl.net.aio import get_loop, run_coroutine_threadsafe
from bndl.net.connection import Connection
from bndl.util.collection import batch


class StorageTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = ''.join(random.choice(string.ascii_lowercase) for _ in range(100 * 1000))


    def setUp(self):
        self.loop = get_loop(start=True)

    def _test_send_on_disk(self, read, write, serialization, compression):
        data_a = list(batch(self.data[0::3], 100))
        data_b = list(batch(self.data[1::3], 100))
        data_c = list(batch(self.data[2::3], 100))

        in_memory = ContainerFactory('memory', serialization, compression)('a')
        on_disk = ContainerFactory('disk', serialization, compression)('b')
        to_disk = ContainerFactory('memory', serialization, compression)('c')

        getattr(in_memory, write)(data_a)
        getattr(on_disk, write)(data_b)
        getattr(to_disk, write)(data_c)
        to_disk.to_disk()

        def connected(reader, writer):
            conn = Connection(self.loop, reader, writer)
            yield from conn.send(in_memory)
            yield from conn.send(on_disk)
            yield from conn.send(to_disk)

        @asyncio.coroutine
        def run_pair():
            server = yield from asyncio.start_server(connected, '0.0.0.0', 0, loop=self.loop)
            socket = server.sockets[0]
            host, port = socket.getsockname()[:2]

            reader, writer = yield from asyncio.open_connection(host, port, loop=self.loop)
            conn = Connection(self.loop, reader, writer)
            a = yield from conn.recv()
            b = yield from conn.recv()
            c = yield from conn.recv()

            self.assertEqual(list(getattr(in_memory, read)()), list(getattr(a, read)()))
            self.assertEqual(list(getattr(on_disk, read)()), list(getattr(b, read)()))
            self.assertEqual(list(getattr(to_disk, read)()), list(getattr(c, read)()))

        run_coroutine_threadsafe(run_pair(), self.loop).result()



    @classmethod
    def _setup_tests(cls):
        rw = (('read', 'write'), ('read_all', 'write_all'))
        serializations = 'pickle', 'marshal', 'json', 'msgpack'
        compressions = (None, 'gzip', 'lz4')

        for (read, write), serialization, compression in product(rw, serializations, compressions):
            args = read, write, serialization, compression
            name = 'test_send_on_disk_' + '_'.join(map(str, args))
            test = lambda self, args = args: self._test_send_on_disk(*args)
            setattr(StorageTest, name, test)

StorageTest._setup_tests()
