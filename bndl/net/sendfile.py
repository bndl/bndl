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

'''
Adapted from and generalized into a utility outside the web context:
https://github.com/KeepSafe/aiohttp/blob/72e615b508dc2def975419da1bddc2e3a0970203/aiohttp/web_urldispatcher.py#L439
'''

import asyncio
import os
import contextlib
from bndl.util import aio


CHUNK_SIZE = 8 * 1024


def file_attachment(filename, offset, size):
    assert hasattr(os, "sendfile")

    @contextlib.contextmanager
    def _attacher():
        @asyncio.coroutine
        def sender(loop, writer):
            '''
            make sure there is no data pending in the writer's buffer
            get the socket from the writer
            use sendfile to send file to socket
            '''
            with open(filename, 'rb') as file:
                yield from aio.drain(writer)
                socket = writer.get_extra_info('socket').dup()
                socket.setblocking(False)
                yield from sendfile(socket.fileno(), file.fileno(), offset, size, loop)
        yield size, sender

    return filename.encode('utf-8'), _attacher



def _sendfile_cb_system(loop, fut, out_fd, in_fd, offset, nbytes, registered):
    if registered:
        loop.remove_writer(out_fd)
    try:
        written = os.sendfile(out_fd, in_fd, offset, nbytes)
        if written == 0:  # EOF reached
            written = nbytes
    except (BlockingIOError, InterruptedError):
        written = 0
    except Exception as exc:
        fut.set_exception(exc)
        return

    if written < nbytes:
        loop.add_writer(out_fd, _sendfile_cb_system,
                        loop, fut, out_fd, in_fd, offset + written, nbytes - written, True)
    else:
        fut.set_result(None)


def _getfd(file):
    if hasattr(file, 'fileno'):
        return file.fileno()
    else:
        return file


@asyncio.coroutine
def sendfile(outf, inf, offset, nbytes, loop=None):
    assert hasattr(os, "sendfile")
    out_fd = _getfd(outf)
    in_fd = _getfd(inf)
    loop = loop or asyncio.get_event_loop()
    fut = asyncio.Future(loop=loop)
    _sendfile_cb_system(loop, fut, out_fd, in_fd, offset, nbytes, False)
    yield from fut
