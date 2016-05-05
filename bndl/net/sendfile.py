'''
Adapted from https://github.com/KeepSafe/aiohttp/blob/72e615b508dc2def975419da1bddc2e3a0970203/aiohttp/web_urldispatcher.py#L439
and generalized into a utility outside the web context
'''


import os
import asyncio
from bndl.util.aio import get_loop
from datetime import datetime



#     resp.set_tcp_cork(True)
#     try:
#         with filepath.open('rb') as f:
#             yield from self._sendfile(request, resp, f, file_size)
#     finally:
#         resp.set_tcp_nodelay(True)





def _sendfile_cb_system(loop, fut, out_fd, in_fd, offset, nbytes, registered):
    if registered:
        loop.remove_writer(out_fd)
    try:
        n = os.sendfile(out_fd, in_fd, offset, nbytes)
        if n == 0:  # EOF reached
            n = nbytes
    except (BlockingIOError, InterruptedError):
        n = 0
    except Exception as exc:
        fut.set_exception(exc)
        return

    if n < nbytes:
        loop.add_writer(out_fd, _sendfile_cb_system,
                        loop, fut, out_fd, in_fd, offset + n, nbytes - n, True)
    else:
        fut.set_result(None)


def _sendfile_cb_fallback(loop, fut, out_fd, in_fd, buffered, registered):
    if registered:
        loop.remove_writer(out_fd)
    chunk_size = 8 * 1024
    if buffered:
        written = os.write(out_fd, buffered)
        buffered = buffered[written:]
    while True:
        if not buffered:
            buffered = os.read(in_fd, chunk_size)
        if not buffered:
            fut.set_result(None)
            return
        written = os.write(out_fd, buffered)
        buffered = buffered[written:]
        if buffered:
            loop.add_writer(out_fd, _sendfile_cb_fallback,
                            loop, fut, out_fd, in_fd, buffered, True)
            return


def _getfd(f):
    if hasattr(f, 'fileno'):
        return f.fileno()
    else:
        return f

@asyncio.coroutine
def sendfile(outf, inf, offset, nbytes, loop=None):
    out_fd = _getfd(outf)
    in_fd = _getfd(inf)
    loop = loop or asyncio.get_event_loop()
    fut = asyncio.Future(loop=loop)
    if hasattr(os, "sendfile"):
        _sendfile_cb_system(loop, fut, out_fd, in_fd, offset, nbytes, False)
    else:
        _sendfile_cb_fallback(loop, fut, out_fd, in_fd, None, False)
    yield from fut



