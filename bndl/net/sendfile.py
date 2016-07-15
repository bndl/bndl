'''
Adapted from and generalized into a utility outside the web context:
https://github.com/KeepSafe/aiohttp/blob/72e615b508dc2def975419da1bddc2e3a0970203/aiohttp/web_urldispatcher.py#L439
'''

import asyncio
import os


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


def _getfd(file):
    if hasattr(file, 'fileno'):
        return file.fileno()
    else:
        return file


@asyncio.coroutine
def sendfile(outf, inf, offset, nbytes, loop=None):
    out_fd = _getfd(outf)
    in_fd = _getfd(inf)
    fut = asyncio.Future(loop=loop)
    if hasattr(os, "sendfile"):
        _sendfile_cb_system(loop, fut, out_fd, in_fd, offset, nbytes, False)
    else:
        _sendfile_cb_fallback(loop, fut, out_fd, in_fd, None, False)
    yield from fut
