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

from concurrent.futures import CancelledError
from datetime import datetime
from functools import lru_cache
from itertools import count
import logging

from bndl.net.connection import NotConnected
from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)



# TODO consider computing status like failed, started, etc.



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

    def __init__(self, ctx, task_id, *, priority=None, name=None, desc=None, group=None, max_attempts=None):
        super().__init__(name or 'task ' + str(task_id),
                         desc or 'unknown task ' + str(task_id))
        self.ctx = ctx
        self.id = task_id
        self.group = group
        self.max_attempts = max_attempts or ctx.conf['bndl.compute.attempts']

        self.priority = task_id if priority is None else priority

        self.dependencies = set()
        self.dependents = set()
        self.blocked = set()
        self.executed_on = []
        self.result_on = []

        self.result = None
        self.exception = None


    def execute(self, scheduler, executor):
        '''
        Execute the task on a executor. The scheduler is provided as 'context' for the task.
        Returns a Future for the task's result.
        '''
        raise NotImplemented()


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
    def succeeded(self):
        return self.stopped and not self.exception


    @property
    def failed(self):
        return self.stopped and self.exception is not None


    @property
    def attempts(self):
        return len(self.executed_on)


    def set_executing(self, executor):
        '''Utility for sub-classes to register the task as executing on a executor.'''
        if self.cancelled:
            raise CancelledError()
        assert not self.running, '%r running' % self
        self.result = None
        self.exception = None
        self.cancelled = False
        self.executed_on.append(executor.name)
        self.result_on.append(executor.name)
        self.signal_start()


    def mark_done(self, result=None):
        ''''
        Externally mark the task as done. E.g. because its 'side effect' (result) is already
        available).
        '''
        self.exception = None
        self.result = result
        if not self.stopped:
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
        if not self.stopped_on:
            self.stopped_on = datetime.now()
        self.result = None
        self.exception = exc
        self.signal_stop()


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


    def reset(self):
        self.result = None
        self.exception = None


    def release(self):
        '''Release most resources of the task. Invoked after a job's execution is complete.'''
        self.result = None
        self.dependencies = set()
        self.dependents = set()
        self.blocked = set()
        if self.executed_on:
            self.executed_on = [self.executed_on[-1]]
        if self.result_on:
            self.result_on = [self.result_on[-1]]
        self.started_listeners.clear()
        self.stopped_listeners.clear()


    def __repr__(self):
        if self.failed:
            state = ' failed'
        elif self.succeeded:
            state = ' succeeded'
        elif self.running:
            state = ' running'
        else:
            state = ''
        return '<%s %s%s>' % (self.__class__.__name__, self.id_str, state)


    @property
    def id_str(self):
        return '.'.join(map(str, self.id)) if isinstance(self.id, tuple) else self.id



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
        assert self.args is not None and self.kwargs is not None
        assert not self.succeeded, '%r already running?' % (self)
        self.set_executing(executor)
        schedule_future = executor.service('tasks').execute_async(self.method, *self.args, **self.kwargs)
        schedule_future.executor = executor
        schedule_future.add_done_callback(self._task_scheduled)


    def _last_executor(self):
        if self.executed_on:
            return self.ctx.node.peers.get(self.executed_on[-1])


    def _task_scheduled(self, schedule_future):
        try:
            handle = schedule_future.result()
        except Exception as exc:
            self.mark_failed(exc)
        else:
            if self.cancelled:
                self._cancel(handle)
            else:
                self.handle = handle
                try:
                    schedule_future.executor.service('tasks') \
                        .get_task_result(self.handle) \
                        .add_done_callback(self._task_completed)
                except NotConnected as exc:
                    self.mark_failed(exc)


    def _task_completed(self, future):
        try:
            self.handle = None
            self.result = future.result()
        except Exception as exc:
            self.exception = exc
        finally:
            self.signal_stop()


    def cancel(self):
        super().cancel()
        if self.handle:
            logger.debug('Canceling %s', self)
            self._cancel(self.handle)
            self.handle = None


    def _cancel(self, handle):
        return self._last_executor().service('tasks').cancel_task(handle)


    def mark_failed(self, exc):
        self.handle = None
        super().mark_failed(exc)


    def release(self):
        super().release()
        self.method = self.method.__name__
        self.handle = None
        self.args = None
        self.kwargs = None
        self.locality = None
