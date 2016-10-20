import asyncio
import ctypes
import logging
import threading

from bndl.execute import TaskCancelled
from bndl.rmi.node import RMINode
from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)


_CURRENT_WORKER = threading.local()
_CURRENT_WORKER_ERR_MSG = '''\
Working outside of task context.

This typically means you attempted to use functionality that needs to interface
with the local worker node.
'''


def current_worker():
    try:
        return _CURRENT_WORKER.w
    except AttributeError:
        raise RuntimeError(_CURRENT_WORKER_ERR_MSG)



class TaskRunner(threading.Thread):
    def __init__(self, worker, task, args, kwargs):
        super().__init__()
        self.worker = worker
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.result = asyncio.Future(loop=worker.loop)


    def run(self):
        # set worker context
        _CURRENT_WORKER.w = self.worker
        try:
            result = self.task(self, *self.args, **self.kwargs)
            self.result.set_result(result)
        except Exception as exc:
            with catch(asyncio.futures.InvalidStateError):
                self.result.set_exception(exc)
        finally:
            # clean up worker context
            del _CURRENT_WORKER.w



class Worker(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks_running = {}


    def run_task(self, src, task, *args, **kwargs):
        logger.debug('running task %s from %s', task, src.name)

        # set worker context
        _CURRENT_WORKER.w = self
        try:
            return task(self, *args, **kwargs)
        finally:
            # clean up worker context
            del _CURRENT_WORKER.w


    @asyncio.coroutine
    def run_task_async(self, src, task, *args, **kwargs):
        logger.debug('running task %s from %s asynchronously', task, src.name)

        runner = TaskRunner(self, task, args, kwargs)
        runner.start()

        task_id = id(runner)
        self.tasks_running[task_id] = runner
        return task_id


    @asyncio.coroutine
    def get_task_result(self, src, task_id):
        logger.debug('waiting for result of task %s for %s', task_id, src.name)
        try:
            task = self.tasks_running.get(task_id)
            return (yield from task.result)
        finally:
            self.tasks_running.pop(task_id, None)


    @asyncio.coroutine
    def cancel_task(self, src, task_id):
        logger.debug('canceling task %s on request of %s', task_id, src.name)
        try:
            task = self.tasks_running.pop(task_id)
        except KeyError:
            return False
        else:
            task.result.cancel()
            thread_id = ctypes.c_size_t(task.ident)
            exc = ctypes.py_object(TaskCancelled)
            modified = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exc)
            return modified > 0
