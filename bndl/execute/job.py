from concurrent.futures import CancelledError, Future, TimeoutError
from datetime import datetime
from functools import lru_cache
from itertools import count
import logging

from bndl.net.connection import NotConnected
from bndl.util.lifecycle import Lifecycle


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

    def __init__(self, ctx, task_id, *, priority=None, name=None, desc=None, group=None):
        super().__init__(name or 'task ' + str(task_id),
                         desc or 'unknown task ' + str(task_id))
        self.ctx = ctx
        self.id = task_id
        self.group = group

        self.priority = task_id if priority is None else priority
        self.future = None

        self.dependencies = []
        self.dependents = []
        self.executed_on = []


    def execute(self, worker):
        pass


    def set_executing(self, worker):
        if self.cancelled:
            raise CancelledError()
        assert not self.pending, '%r pending' % self
        self.executed_on.append(worker.name)
        self.signal_start()


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


    def mark_done(self, result=None):
        if not self.done:
            future = self.future = Future()
            future.set_result(result)
            if not self.started_on:
                self.started_on = datetime.now()
            self.signal_stop()


    @property
    def pending(self):
        return self.future and not self.future.done()


    @property
    def succeeded(self):
        return self.future and not self.failed


    @property
    def failed(self):
        try:
            return bool(self.future and self.future.exception(0))
        except (CancelledError, TimeoutError):
            return False


    def mark_failed(self, exc):
        future = self.future = Future()
        future.set_exception(exc)
        self.signal_stop()


    def result(self):
        assert self.future, 'task %r not yet scheduled' % self
        return self.future.result()


    def exception(self):
        assert self.future, 'task %r not yet started' % self
        return self.future.exception()


    @property
    def executed_on_last(self):
        if self.executed_on:
            return self.executed_on[-1]


    def release(self):
        if not self.failed:
            self.future = None
        self.dependencies = []
        self.dependents = []
        if self.executed_on:
            self.executed_on = [self.executed_on[-1]]
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

    def __init__(self, ctx, task_id, method, args=(), kwargs=None, *, priority=None, name=None, desc=None, group=None):
        super().__init__(ctx, task_id, priority=priority, name=name, desc=desc, group=group)
        self.method = method
        self.args = args
        self.kwargs = kwargs or {}
        self.handle = None


    def execute(self, worker):
        self.set_executing(worker)
        future = self.future = Future()
        future2 = worker.execute_async(self.method, *self.args, **self.kwargs)
        # TODO remove future.worker, just for checking
        future2.worker = worker
        # TODO put time sleep here to test what happens if task
        # is done before adding callback (callback runs in this thread)
        future2.add_done_callback(self._task_scheduled)
        return future

    @property
    def _last_worker(self):
        if self.executed_on:
            return self.ctx.node.peers.get(self.executed_on[-1])


    def _task_scheduled(self, future):
        try:
            self.handle = future.result()
        except Exception as exc:
            self.mark_failed(exc)
        else:
            try:
                # TODO remove future.worker
                # assert future.worker == self._last_worker, '%r != %r' % (future.worker, self._last_worker)
                future = self._last_worker.get_task_result(self.handle)
                # TODO put time sleep here to test what happens if task
                # is done before adding callack (callback gets executed in this thread)
                future.add_done_callback(self._task_completed)
            except NotConnected as exc:
                self.mark_failed(exc)


    def _task_completed(self, future):
        try:
            self.handle = None
            result = future.result()
        except Exception as exc:
            if self.future:
                self.future.set_exception(exc)
            elif not isinstance(exc, NotConnected):
                logger.info('execution of %s on %s failed, but not expecting result',
                            self, self.executed_on_last, exc_info=True)
        else:
            if self.future and not self.future.cancelled():
                self.future.set_result(result)
            else:
                logger.info('task %s (%s) completed, but not expecting result')
        finally:
            self.signal_stop()


    def cancel(self):
        super().cancel()

        if self.handle:
            logger.debug('canceling %s', self)
            self._last_worker.cancel_task(self.handle)
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
