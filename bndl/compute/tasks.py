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
import ctypes
import logging
import threading

from bndl.compute.exceptions import TaskCancelled


logger = logging.getLogger(__name__)


_TASK_CTX = threading.local()
_TASK_CTX_ERR_MSG = '''\
Working outside of task context.

This typically means you attempted to use functionality that needs to interface
with the local node.
'''


def task_context():
    ctx = _TASK_CTX
    if not hasattr(ctx, 'data'):
        raise RuntimeError(_TASK_CTX_ERR_MSG)
    return ctx.data


def current_node():
    try:
        return _TASK_CTX.node
    except AttributeError:
        raise RuntimeError(_TASK_CTX_ERR_MSG)


class TaskExecutor(threading.Thread):
    def __init__(self, tasks, task, args, kwargs):
        super().__init__(name='task-executor')
        self.tasks = tasks
        self.node = tasks.node
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.result = asyncio.Future(loop=tasks.node.loop)


    def run(self):
        try:
            result = self.tasks._execute(self.task, *self.args, **self.kwargs)
            exc = None
        except Exception as e:
            exc = e
            logger.info('Unable to execute %s', self.task, exc_info=True)

        if not self.result.cancelled():
            try:
                if exc:
                    self.node.loop.call_soon_threadsafe(self.result.set_exception, exc)
                else:
                    self.node.loop.call_soon_threadsafe(self.result.set_result, result)
            except RuntimeError:
                logger.warning('Unable to send response for task %s', self.task)



class Tasks(object):
    def __init__(self, node):
        self.node = node
        self.tasks = {}


    def _execute(self, task, *args, **kwargs):
        # set executor context
        _TASK_CTX.node = self.node
        _TASK_CTX.data = {}
        try:
            return task(*args, **kwargs)
        finally:
            # clean up executor context
            del _TASK_CTX.node
            del _TASK_CTX.data


    def execute(self, src, task, *args, **kwargs):
        logger.debug('Executing task %s from %s', task, src.name)
        return self._execute(task, *args, **kwargs)


    @asyncio.coroutine
    def execute_async(self, src, task, *args, **kwargs):
        executor = TaskExecutor(self, task, args, kwargs)
        task_id = id(executor)
        self.tasks[task_id] = executor

        logger.debug('Executing task %s from %s asynchronously with id %r',
                     task, src.name, task_id)
        executor.start()
        return task_id


    @asyncio.coroutine
    def get_task_result(self, src, task_id):
        logger.debug('Collecting result of task %r for %r', task_id, src.name)
        try:
            task = self.tasks[task_id]
            return (yield from task.result)
        except KeyError:
            logger.error('No task with id %r', task_id)
            raise
        finally:
            self.tasks.pop(task_id, None)


    @asyncio.coroutine
    def cancel_task(self, src, task_id):
        logger.debug('Canceling task %r on request of %r', task_id, src.name)
        try:
            task = self.tasks.pop(task_id)
        except KeyError:
            return False
        else:
            task.result.cancel()
            thread_id = ctypes.c_size_t(task.ident)
            exc = ctypes.py_object(TaskCancelled)
            modified = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exc)
            return modified > 0
