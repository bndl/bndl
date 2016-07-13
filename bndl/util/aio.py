import asyncio
import functools
import logging

import concurrent.futures


logger = logging.getLogger(__name__)


def get_loop(stop_on=()):
    '''
    Get the current asyncio loop. If none exists (e.g. not on the main thread)
    a new loop is created.
    :param stop_on:
    '''
    try:
        loop = asyncio.get_event_loop()
    except (AssertionError, RuntimeError):
        # unfortunately there is no way to tell if there is already an event loop
        # active apart from trying to get it, and catching the assertion error
        # raised if there is no active loop.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if stop_on:
        for sig in stop_on:
            loop.add_signal_handler(sig, loop.stop)
    return loop


def async_call(loop, method, *args, **kwargs):
    '''
    Provide a coroutine which can be yielded from, be it from a coroutine
    function or from a plain function / method. In the later case it is executed
    on the default executor of the asyncio loop.
    :param loop: asyncio event loop
    :param method: coroutine function, function or method
    '''
    if asyncio.iscoroutinefunction(method):
        return method(*args, **kwargs)
    else:
        if kwargs:
            method = functools.partial(method, **kwargs)
        return loop.run_in_executor(None, method, *args)


def run_coroutine_threadsafe(coro, loop):
    '''
    Run a asyncio coroutine in the given loop safely and return a
    concurrent.futures.Future
    :param coro: asyncio.couroutine
    :param loop: asyncio loop
    '''
    future = concurrent.futures.Future()
    scheduled = concurrent.futures.Future()

    def task_done(task):
        try:
            future.set_result(task.result())
        except Exception as exc:
            logger.debug('task failed', exc_info=True)
            future.set_exception(exc)


    def schedule_task():
        try:
            logger.debug('scheduling coro %s as task', coro)
            task = loop.create_task(coro)
            task.add_done_callback(task_done)
            scheduled.set_result(task)
        except Exception as exc:
            logger.exception('unable to schedule task')
            future.set_exception(exc)

    schedule_task_handle = loop.call_soon_threadsafe(schedule_task)

    def cancel_task():
        scheduled.result().cancel()

    def future_done(future):
        try:
            schedule_task_handle.cancel()
            loop.call_soon_threadsafe(cancel_task)
        except Exception:
            logger.exception('canceling future failed')
    future.add_done_callback(future_done)

    return future


@asyncio.coroutine
def drain(writer):
    if writer.transport.get_write_buffer_size():
        low, high = writer.transport.get_write_buffer_limits()
        writer.transport.set_write_buffer_limits(low=0)
        yield from writer.drain()
        writer.transport.set_write_buffer_limits(high, low)
