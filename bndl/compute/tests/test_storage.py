from unittest.case import TestCase
import asyncio
import random
import string

from bndl.compute.storage import StorageContainerFactory
from bndl.net.connection import Connection
from bndl.util.aio import get_loop


class StorageTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = ''.join(random.choice(string.ascii_lowercase) for _ in range(100 * 1000)).encode('utf-8')

    def setUp(self):
        self.loop = get_loop()


    def test_send_on_disk(self):
        in_memory = StorageContainerFactory('memory', 'pickle')('a')
        on_disk = StorageContainerFactory('disk', 'pickle')('b')
        to_disk = StorageContainerFactory('memory', 'pickle')('c')

        in_memory.write(self.data[0::3])
        on_disk.write(self.data[1::3])
        to_disk.write(self.data[2::3])
        to_disk.to_disk()

        def connected(reader, writer):
            conn = Connection(self.loop, reader, writer)
            yield from conn.send(in_memory)
            yield from conn.send(on_disk)
            yield from conn.send(to_disk)

        @asyncio.coroutine
        def run_pair():
            server = yield from asyncio.start_server(connected, '::', 0)
            print('started server')
            socket = server.sockets[0]
            host, port = socket.getsockname()[:2]

            reader, writer = yield from asyncio.open_connection(host, port, loop=self.loop)
            conn = Connection(self.loop, reader, writer)
            a = yield from conn.recv()
            b = yield from conn.recv()
            c = yield from conn.recv()

            self.assertEqual(in_memory.read(), a.read())
            self.assertEqual(on_disk.read(), b.read())
            self.assertEqual(to_disk.read(), c.read())

        self.loop.run_until_complete(run_pair())