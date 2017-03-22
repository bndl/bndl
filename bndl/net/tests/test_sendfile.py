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
import os
import random
import socket
import string
import tempfile
import threading

from bndl.net.sendfile import sendfile
from bndl.util.aio import get_loop, run_coroutine_threadsafe


class ConnectionTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = ''.join(random.choice(string.ascii_lowercase) for _ in range(500 * 1000)).encode('utf-8')


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
        loop = get_loop(start=True)
        run_coroutine_threadsafe(sendfile(sendsock.fileno(), src.file.fileno(), 0, len(self.data), loop=loop), loop=loop).result()
        receiver.join()
        received = b''.join(received)
        self.assertEqual(self.data, received)


    def test_copyfile(self):
        src = tempfile.NamedTemporaryFile(prefix='bndl-sendfiletest-')
        src.file.write(self.data)
        src.file.seek(0)
        size = os.stat(src.fileno()).st_size

        dst = tempfile.NamedTemporaryFile(prefix='bndl-sendfiletest-')

        loop = get_loop(start=True)
        run_coroutine_threadsafe(sendfile(dst.file.fileno(), src.file.fileno(), 0, size, loop=loop), loop=loop).result()
        self.assertEqual(size, os.stat(dst.fileno()).st_size)

        src.file.seek(0)
        dst.file.seek(0)

        self.assertEqual(src.file.read(), dst.file.read())

        src.close()
        dst.close()
