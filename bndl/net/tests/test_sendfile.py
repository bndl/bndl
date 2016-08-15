import os
import random
import socket
import string
import tempfile
import threading
from unittest.case import TestCase

from bndl.net.sendfile import sendfile
from bndl.util.aio import get_loop


class ConnectionTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = ''.join(random.choice(string.ascii_lowercase) for _ in range(500 * 1000)).encode('utf-8')

    def setUp(self):
        self.loop = get_loop()


    def test_sendfile(self):
        sendsock, recvsock = socket.socketpair()

        src = tempfile.NamedTemporaryFile(prefix='bndl-sendfiletest-')
        src.file.write(self.data)

        received = []
        def receive():
            while sum(len(buffer) for buffer in received) < len(self.data):
                buffer = recvsock.recv(len(self.data))
                received.append(buffer)
        receiver = threading.Thread(target=receive)
        receiver.start()
        loop = get_loop()
        loop.run_until_complete(sendfile(sendsock.fileno(), src.file.fileno(), 0, len(self.data), loop))
        receiver.join()
        received = b''.join(received)
        self.assertEqual(self.data, received)


    def test_copyfile(self):
        src = tempfile.NamedTemporaryFile(prefix='bndl-sendfiletest-')
        src.file.write(self.data)
        src.file.seek(0)
        size = os.stat(src.fileno()).st_size

        dst = tempfile.NamedTemporaryFile(prefix='bndl-sendfiletest-')

        loop = get_loop()
        loop.run_until_complete(sendfile(dst.file.fileno(), src.file.fileno(), 0, size, loop))
        self.assertEqual(size, os.stat(dst.fileno()).st_size)

        src.file.seek(0)
        dst.file.seek(0)

        self.assertEqual(src.file.read(), dst.file.read())

        src.close()
        dst.close()
