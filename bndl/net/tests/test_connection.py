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

from unittest.case import TestCase
import asyncio
import contextlib

from bndl.net import serialize
from bndl.net.aio import get_loop, run_coroutine_threadsafe
from bndl.net.connection import Connection
from bndl.net.messages import Hello


class WithAttachment(object):
    def __init__(self, name, body):
        self.name = name
        self.body = body

    def __getstate__(self):
        body = self.body.encode('utf-8')
        @contextlib.contextmanager
        def write(loop, writer):
            def _write():
                writer.write(body)
            yield len(body), _write
        serialize.attach(self.name.encode('utf-8'), write)
        return dict(name=self.name)

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.body = serialize.attachment(self.name.encode('utf-8')).decode()




class ConnectionTest(TestCase):
    def setUp(self):
        self.conns = [None] * 2
        self.server = None
        self.loop = get_loop(start=True)

        def serve(reader, writer):
            self.conns[1] = Connection(self.loop, reader, writer)

        @asyncio.coroutine
        def connect():
            host, port = '127.0.0.10', 5000
            # let a server listen
            self.server = (yield from asyncio.start_server(serve, host, port, loop=self.loop))
            # connect a client
            reader, writer = yield from asyncio.open_connection(host, port, loop=self.loop)
            self.conns[0] = Connection(self.loop, reader, writer)

        def close():
            if self.server:
                self.server.close()
            for conn in self.conns:
                if conn:
                    run_coroutine_threadsafe(conn.close(), loop=self.loop).result()

        self.addCleanup(close)
        run_coroutine_threadsafe(connect(), loop=self.loop).result()


    def send(self, conn, msg):
        run_coroutine_threadsafe(conn.send(msg, drain=False), loop=self.loop).result()


    def recv(self, conn, timeout=None):
        @asyncio.coroutine
        def _recv():
            return (yield from conn.recv(timeout))
        return run_coroutine_threadsafe(_recv(), loop=self.loop).result()


    def test_marshallable(self):
        hello = Hello(name='test')
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]), hello)


    def test_picklable(self):
        hello = Hello(name=Hello(name='test'))
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]), hello)


    def test_cloudpicklable(self):
        hello = Hello(name=lambda: 'test')
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]).name(), hello.name())


    def test_attachment(self):
        obj = WithAttachment('test', 'body' * 1000 * 1000)
        hello = Hello(name=obj)
        self.send(self.conns[0], hello)
        obj2 = self.recv(self.conns[1]).name
        self.assertEqual(obj.name, obj2.name)
        self.assertEqual(obj.body, obj2.body)
