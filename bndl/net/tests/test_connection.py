import asyncio
from unittest.case import TestCase

from bndl.net.connection import Connection
from bndl.net.messages import Hello
from bndl.util.aio import get_loop
from bndl.net import serialize


class WithAttachment(object):
    def __init__(self, name, body):
        self.name = name
        self.body = body

    def __getstate__(self):
        body = self.body.encode('utf-8')
        def write(writer):
            writer.write(body)
        serialize.attach(self.name.encode('utf-8'), len(body), write)
        return dict(name=self.name)

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.body = serialize.attachment(self.name.encode('utf-8')).decode()




class ConnectionTest(TestCase):
    def setUp(self):
        self.conns = [None] * 2
        self.server = None
        self.loop = get_loop()

        def serve(reader, writer):
            self.conns[1] = Connection(self.loop, reader, writer)

        @asyncio.coroutine
        def connect():
            host, port = 'localhost' , 5000
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
                    self.loop.run_until_complete(conn.close())

        self.addCleanup(close)
        self.loop.run_until_complete(connect())


    def send(self, conn, msg):
        self.loop.run_until_complete(conn.send(msg))


    def recv(self, conn, timeout=None):
        holder = [None]
        @asyncio.coroutine
        def _recv():
            holder[0] = yield from conn.recv(timeout)
        self.loop.run_until_complete(_recv())
        return holder[0]


    def test_marshallable(self):
        hello = Hello(name='test')
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]), hello)


    def test_picklable(self):
        hello = Hello(name=Hello(name='test'))
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]), hello)


    def test_cloudpicklable(self):
        hello = Hello(name=lambda:'test')
        self.send(self.conns[0], hello)
        self.assertEqual(self.recv(self.conns[1]).name(), hello.name())


    def test_attachment(self):
        obj = WithAttachment('test', 'body')
        hello = Hello(name=obj)
        self.send(self.conns[0], hello)
        obj2 = self.recv(self.conns[1]).name
        self.assertEqual(obj.name, obj2.name)
        self.assertEqual(obj.body, obj2.body)


