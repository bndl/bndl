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

import asyncio
import atexit
import concurrent.futures
import functools
import logging
import threading
import time

from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)



def exception_handler(loop, context):
    exc = context.get('exception')
    if type(exc) is not SystemExit:
        loop.default_exception_handler(context)


_loop = None
_loop_lock = threading.RLock()
_loop_thread = None


def get_loop(stop_on=(), use_uvloop=True, start=False):
    '''
    Get the current asyncio loop. If none exists (e.g. not on the main thread)
    a new loop is created.
    :param stop_on:
    '''
    global _loop, _loop_lock

    if _loop:
        assert _loop.is_running()
        return _loop

    with _loop_lock:
        if _loop:
            assert _loop.is_running()
            return _loop

        if not start:
            raise Exception('Asyncio loop not running')

        if use_uvloop:
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except Exception:
                logger.debug('uvloop not available, using default event loop')

        try:
            del asyncio.Task.__del__
        except (AttributeError, TypeError):
            pass

        try:
            loop = asyncio.get_event_loop()
        except (AssertionError, RuntimeError):
            # unfortunately there is no way to tell if there is already an event loop
            # active apart from trying to get it, and catching the assertion error
            # raised if there is no active loop.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.set_exception_handler(exception_handler)

        if stop_on:
            for sig in stop_on:
                loop.add_signal_handler(sig, loop.stop)

        started = threading.Event()

        def run():
            loop.call_soon_threadsafe(started.set)
            loop.run_forever()

        global _loop_thread
        _loop_thread = threading.Thread(target=run, name='asyncio-loop', daemon=True)
        _loop_thread.start()
        started.wait()

        _loop = loop

    assert loop.is_running()
    return loop


def stop_loop():
    with _loop_lock:
        try:
            loop = get_loop()
        except:
            # loop not running
            pass
        else:
            # stop and close loop
            loop.stop()
            for _ in range(10):
                if not loop.is_running():
                    break
                    loop.close()
                time.sleep(.1)

        _loop = None
        _loop_thread = None


def get_loop_thread():
    return _loop_thread


class IOTasks(object):
    def __init__(self):
        self._iotasks = set()
        atexit.register(self._stop_tasks)


    def _create_task(self, coro):
        task = self.loop.create_task(coro)
        self._iotasks.add(task)
        task.add_done_callback(self._iotasks.discard)


    def _stop_tasks(self):
        # cancel any pending io work
        for task in list(self._iotasks):
            with catch():
                task.cancel()



def async_call(loop, executor, method, *args, **kwargs):
    '''
    Provide a coroutine which can be yielded from, be it from a coroutine
    function or from a plain function / method. In the later case it is executed
    on the executor provided.
    :param loop: asyncio event loop
    :param executor: concurrent.futures.Executor
    :param method: coroutine function, function or method
    '''
    if asyncio.iscoroutinefunction(method):
        return method(*args, **kwargs)
    else:
        if kwargs:
            method = functools.partial(method, **kwargs)
        return loop.run_in_executor(executor, method, *args)


def run_coroutine_threadsafe(coro, loop):
    '''
    Run a asyncio coroutine in the given loop safely and return a
    concurrent.futures.Future
    :param coro: asyncio.couroutine
    :param loop: asyncio loop
    '''
    future = concurrent.futures.Future()
    task = None

    def task_done(task):
        try:
            future.set_result(task.result())
        except Exception as exc:
            future.set_exception(exc)

    def schedule_task():
        global task
        try:
            task = loop.create_task(coro)
            task.add_done_callback(task_done)
        except Exception as exc:
            future.set_exception(exc)

    schedule_task_handle = loop.call_soon_threadsafe(schedule_task)

    def propagate_cancel(future):
        if future.cancelled:
            try:
                schedule_task_handle.cancel()
                if task:
                    loop.call_soon_threadsafe(task.cancel)
            except Exception:
                logger.exception('canceling future failed')

    future.add_done_callback(propagate_cancel)

    return future


@asyncio.coroutine
def drain(writer):
    '''
    StreamWriter.drain only drains when there is more data buffered than the
    low_water mark. This method performs a full drain.
    '''
    transport = writer.transport
    if transport.get_write_buffer_size():
        low, high = transport.get_write_buffer_limits()
        transport.set_write_buffer_limits(low=0, high=0)
        yield from writer.drain()
        transport.set_write_buffer_limits(high, low)
    assert transport.get_write_buffer_size() == 0, 'Not all data was drained (%s bytes remain)' % \
                                                   transport.get_write_buffer_size()



@asyncio.coroutine
def readexactly(self, n):
    '''
    Implementation of StreamReader.readexactly which has an O(n) instead of
    O(2n) memory footprint (as is the case for asyncio in python 3.5.2). Also
    it is 25% to 35% faster in terms of throughput. See also
    https://github.com/python/asyncio/issues/394.
    '''
    if self._exception is not None:
        raise self._exception

    if not n:
        return b''

    if len(self._buffer) >= n and not self._eof:
        data = self._buffer[:n]
        del self._buffer[:n]
        self._maybe_resume_transport()
        return data

    data = bytearray(n)
    pos = 0

    while n:
        if not self._eof and not self._buffer:
            yield from self._wait_for_data('readexactly')

        if self._eof or not self._buffer:
            raise asyncio.IncompleteReadError(data[:pos], pos + n)

        available = len(self._buffer)
        if available <= n:
            data[pos:pos + available] = self._buffer
            self._buffer.clear()
            n -= available
            pos += available
        else:
            data[pos:pos + n] = self._buffer[:n]
            del self._buffer[:n]
            n = 0
        self._maybe_resume_transport()

    return data
