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

from concurrent.futures import CancelledError, Future, TimeoutError
from datetime import datetime
from functools import lru_cache
from itertools import count
import logging

from bndl.net.connection import NotConnected
from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)



class Job(Lifecycle):
    '''
    A set of :class:`Tasks <Task>` which can be executed on a cluster of workers / executors.
    '''
    _job_ids = count(1)

    def __init__(self, ctx, tasks, name=None, desc=None):
        super().__init__(name, desc)
        self.id = next(self._job_ids)
        self.ctx = ctx
        self.tasks = tasks


    def cancel(self):
        for task in self.tasks:
            task.cancel()
        super().cancel()


    @lru_cache()
    def group(self, name):
        return [t for t in self.tasks if t.group == name]



class Task(Lifecycle):
    '''
    Execution of a Task on a executor is the basic unit of scheduling in ``bndl.compute``.
    '''

    def __init__(self, ctx, task_id, *, priority=None, name=None, desc=None, group=None):
        super().__init__(name or 'task ' + str(task_id),
                         desc or 'unknown task ' + str(task_id))
        self.ctx = ctx
        self.id = task_id
        self.group = group

        self.priority = task_id if priority is None else priority
        self.future = None

        self.dependencies = set()
        self.dependents = set()
        self.executed_on = []
        self.result_on = []
        self.attempts = 0


    def execute(self, scheduler, executor):
        '''
        Execute the task on a executor. The scheduler is provided as 'context' for the task.
        '''


    def cancel(self):
        '''
        Cancel execution (if not already done) of this task.
        '''
        if not self.done:
            super().cancel()


    def locality(self, executors):
        '''
        Indicate locality for executing this task on executors.

        Args:
            executors (sequence[executor]): The executors to determine the locality for.

        Returns:
            A sequence of executor - locality tuples. 0 is indifferent and can be skipped, -1 is
            forbidden, 1+ increasing locality.
        '''
        return ()


    @property
    def started(self):
        '''Whether the task has started'''
        return bool(self.future)


    @property
    def done(self):
        '''Whether the task has completed execution'''
        fut = self.future
        return fut and fut.done()


    @property
    def pending(self):
        fut = self.future
        return fut and not fut.done()


    @property
    def succeeded(self):
        try:
            fut = self.future
            return bool(fut and fut.result(0))
        except Exception:
            return False


    @property
    def failed(self):
        try:
            fut = self.future
            return bool(fut and fut.exception(0))
        except (CancelledError, TimeoutError):
            return False


    def set_executing(self, executor):
        '''Utility for sub-classes to register the task as executing on a executor.'''
        if self.cancelled:
            raise CancelledError()
        assert not self.pending, '%r pending' % self
        self.executed_on.append(executor.name)
        self.result_on.append(executor.name)
        self.attempts += 1
        self.signal_start()


    def mark_done(self, result=None):
        ''''
        Externally mark the task as done. E.g. because its 'side effect' (result) is already
        available).
        '''
        if not self.done:
            future = self.future = Future()
            future.set_exception(None)
            future.set_result(result)
            if not self.stopped_on:
                self.stopped_on = datetime.now()
            if not self.started_on:
                self.started_on = self.stopped_on
            self.signal_stop()


    def mark_failed(self, exc):
        '''
        'Externally' mark the task as failed. E.g. because the executor which holds the tasks' result
        has failed / can't be reached.
        '''
        future = self.future = Future()
        future.set_exception(exc)
        self.signal_stop()


    def result(self):
        '''
        Get the result of the task (blocks). Raises an exception if the task failed with one.
        '''
        assert self.future, 'task %r not yet scheduled' % self
        return self.future.result()


    def exception(self, timeout=-1):
        '''Get the exception of the task (blocks).'''
        fut = self.future
        assert fut, 'task %r not yet started' % self
        if timeout == -1:
            return fut.exception()
        else:
            try:
                return fut.exception(timeout)
            except (CancelledError, TimeoutError):
                return False


    def last_executed_on(self):
        '''The name of the executor this task executed on last (if any).'''
        try:
            return self.executed_on[-1]
        except IndexError:
            return None


    def last_result_on(self):
        '''The name of the node which has the last result (if any).'''
        try:
            return self.result_on[-1]
        except IndexError:
            return None


    def release(self):
        '''Release most resources of the task. Invoked after a job's execution is complete.'''
        if self.succeeded:
            self.future = None
        self.dependencies = []
        self.dependents = []
        if self.executed_on:
            self.executed_on = [self.executed_on[-1]]
        if self.result_on:
            self.result_on = [self.result_on[-1]]
        self.started_listeners.clear()
        self.stopped_listeners.clear()


    def __repr__(self):
        task_id = '.'.join(map(str, self.id)) if isinstance(self.id, tuple) else self.id
        if self.failed:
            state = ' failed'
        elif self.done:
            state = ' done'
        elif self.pending:
            state = ' pending'
        else:
            state = ''
        return '<%s %s%s>' % (self.__class__.__name__, task_id, state)



class RmiTask(Task):
    '''
    A task which performs a Remote Method Invocation to execute a method with positional and keyword arguments.
    '''

    def __init__(self, ctx, task_id, method, args=(), kwargs=None, *, priority=None, name=None, desc=None, group=None):
        super().__init__(ctx, task_id, priority=priority, name=name, desc=desc, group=group)
        self.method = method
        self.args = args
        self.kwargs = kwargs or {}
        self.handle = None


    def execute(self, scheduler, executor):
        assert not self.handle and not self.succeeded
        self.set_executing(executor)
        result_future = self.future = Future()
        schedule_future = executor.service('tasks').execute_async(self.method, *self.args, **self.kwargs)
        schedule_future.executor = executor
        schedule_future.add_done_callback(self._task_scheduled)
        return result_future


    def _last_executor(self):
        if self.executed_on:
            return self.ctx.node.peers.get(self.executed_on[-1])


    def _task_scheduled(self, schedule_future):
        try:
            self.handle = schedule_future.result()
        except Exception as exc:
            self.mark_failed(exc)
        else:
            try:
                result_future = schedule_future.executor.service('tasks').get_task_result(self.handle)
                result_future.add_done_callback(self._task_completed)
            except NotConnected as exc:
                self.mark_failed(exc)


    def _task_completed(self, future):
        fut = self.future
        try:
            self.handle = None
            result = future.result()
        except Exception as exc:
            if fut:
                fut.set_exception(exc)
            elif not isinstance(exc, NotConnected):
                if logger.isEnabledFor(logging.INFO):
                    logger.info('execution of %s on %s failed, but not expecting result',
                                self, self.last_executed_on(), exc_info=True)
        else:
            if fut and not fut.cancelled():
                fut.set_exception(None)
                fut.set_result(result)
            else:
                logger.info('task %s (%s) completed, but not expecting result')
        finally:
            self.signal_stop()


    def cancel(self):
        super().cancel()

        if self.handle:
            logger.debug('canceling %s', self)
            self._last_executor().service('tasks').cancel_task(self.handle)
            self.handle = None

        if self.future:
            self.future = None


    def release(self):
        super().release()
        self.method = self.method.__name__
        self.handle = None
        self.args = None
        self.kwargs = None
        self.locality = None
