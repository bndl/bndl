from concurrent.futures import CancelledError, Future, TimeoutError
from functools import lru_cache
from itertools import count
import logging

from ..util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)



class Job(Lifecycle):
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

    def __init__(self, ctx, task_id, *, priority=None, name=None, desc=None):
        super().__init__(name or 'task ' + str(task_id),
                         desc or 'unknown task ' + str(task_id))
        self.ctx = ctx
        self.id = task_id

        self.priority = task_id if priority is None else priority
        self.future = None

        self.dependencies = []
        self.dependents = []
        self.executed_on = []


    def execute(self, worker):
        pass


    def cancel(self):
        if not self.done:
            super().cancel()


    def locality(self, workers):
        '''
        Indicate locality for executing this task on workers.
        :param workers: The workers to determine the locality for.
        :return: Sequence[(worker, locality), ...].
            A sequence of worker - locality tuples. 0 is indifferent and can be
            skipped, -1 is forbidden, 1+ increasing locality.
        '''
        return ()


    @property
    def started(self):
        return bool(self.future)


    @property
    def done(self):
        return self.future and self.future.done()


    @property
    def failed(self):
        try:
            return self.future and self.future.exception(0)
        except (CancelledError, TimeoutError):
            return False


    def result(self):
        assert self.future, 'task not yet scheduled'
        try:
            return self.future.result()
        finally:
            self._release_resources()


    def exception(self):
        assert self.future, 'task not yet started'
        return self.future.exception()


    def _future_done(self, future):
        self.signal_stop()


    def release(self):
        self.future = None
        self.dependencies = None
        self.dependents = None
        if self.executed_on:
            self.executed_on = [self.executed_on[-1]]


    def __repr__(self):
        task_id = '.'.join(map(str, self.id)) if isinstance(self.id, tuple) else self.id
        if self.failed:
            state = ' failed'
        elif self.done:
            state = ' done'
        elif self.running:
            state = ' running'
        else:
            state = ''
        return '<Task %s%s>' % (task_id, state)



class RemoteTask(Task):

    def __init__(self, ctx, task_id, method, args=(), kwargs=None, *, priority=None, name=None, desc=None, group=None):
        super().__init__(ctx, task_id, priority=priority, name=name, desc=desc)
        self.method = method
        self.args = args
        self.kwargs = kwargs or {}
        self.handle = None
        self.group = group


    def execute(self, worker):
        if self.cancelled:
            raise CancelledError()
        args = self.args
        kwargs = self.kwargs

        self.signal_start()
        self.executed_on.append(worker)

        future = self.future = Future()
        future2 = worker.run_task_async(self.method, *args, **kwargs)
        # TODO put time sleep here to test what happens if task
        # is done before adding callback (callback gets executed in this thread)
        future2.add_done_callback(self._task_scheduled)
        return future


    def _task_scheduled(self, future):
        try:
            self.handle = future.result()
        except Exception as exc:
            if self.future:
                self.future.set_exception(exc)
            else:
                logger.info('scheduling %s on %s failed, but not expecting task to be scheduled', self, self.executed_on[-1], exc_info=True)
        else:
            future = self.executed_on[-1].get_task_result(self.handle)
            # TODO put time sleep here to test what happens if task
            # is done before adding callack (callback gets executed in this thread)
            future.add_done_callback(self._task_completed)


    def _task_completed(self, future):
        try:
            result = future.result()
        except Exception as exc:
            if self.future:
                self.future.set_exception(exc)
            else:
                logger.info('execution of %s on %s failed, but not expecting result', self, self.executed_on[-1], exc_info=True)
        else:
            if self.future and not self.future.cancelled():
                self.future.set_result(result)
            else:
                logger.info('task %s (%s) completed, but not expecting result')
        finally:
            self.signal_stop()


    def cancel(self):
        if not self.done:
            try:
                logger.debug('canceling %s', self)
                if self.handle:
                    self.executed_on[-1].cancel_task(self.handle)
                if self.future:
                    self.future.cancel()
                super().cancel()
            finally:
                self.future = None


    def result(self):
        assert self.future, 'task not yet scheduled'
        return self.future.result()


    def release(self):
        super().release()
        self.method = None
        self.handle = None
        self.args = None
        self.kwargs = None
