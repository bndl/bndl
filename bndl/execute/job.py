import abc
import functools
from itertools import chain
import logging
import queue

from bndl.util.lifecycle import Lifecycle
import itertools
from collections import Counter
import threading


logger = logging.getLogger(__name__)


class Job(Lifecycle):

    _jobids = itertools.count(1)

    def __init__(self, ctx, name=None, desc=None):
        super().__init__(name, desc)
        self.id = next(self._jobids)
        self.ctx = ctx
        self.stages = []


    def execute(self, workers, eager=True):
        self.signal_start()

        logger.info('executing job %s with stages %s', self, self.stages)

        try:
            for stage in self.stages:
                if not self.running:
                    break
                logger.info('executing stage %s with %s tasks', stage, len(stage.tasks))
                stage.add_listener(self._stage_done)
                yield stage.execute(workers, eager)
        except:
            self.signal_stop()
            raise


    def _stage_done(self, stage):
        if stage.stopped and stage == self.stages[-1]:
            self.signal_stop()


    def cancel(self):
        super().cancel()
        for stage in self.stages:
            stage.cancel()


    @property
    def tasks(self):
        return list(chain.from_iterable(stage.tasks for stage in self.stages))



@functools.total_ordering
class Stage(Lifecycle):

    def __init__(self, stage_id, job, name=None, desc=None):
        super().__init__(name, desc)
        self.id = stage_id
        self.job = job
        self.tasks = []

    def execute(self, workers, eager=True):
        self.signal_start()

        try:
            if eager:
                yield from self._execute_eagerly(workers)
            else:
                yield from self._execute_onebyone(workers)
        except (Exception, KeyboardInterrupt):
            self.cancelled = True
            raise
        finally:
            for task in self.tasks:
                if not task.stopped:
                    task.cancel()
            self.signal_stop()

    def _execute_eagerly(self, workers):
        occupied = set()
        workers_available = threading.Semaphore(len(workers))
        task_results = queue.Queue()
        tasks_todo = self.tasks[::-1]
        tasks_pending = len(tasks_todo)
        task_counts = Counter()

        def task_done(worker, task):
            task_results.put((worker, task))
            workers_available.release()

        def start_task(worker, task):
            occupied.add(worker)
            task_counts[worker.name] += 1
            future = task.execute(worker)
            future.add_done_callback(lambda future: task_done(worker, task))

        while tasks_pending:
            if tasks_todo:
                workers_available.acquire()
                task = tasks_todo.pop()
                for worker in chain.from_iterable((task.preferred_workers or (), task.allowed_workers or workers)):
                    if worker not in occupied:
                        break
                    else:
                        worker = None
                if worker:
                    start_task(worker, task)
                else:
                    tasks_todo.insert(0, task)

            # yield available results
            try:
                worker, task = task_results.get_nowait() if tasks_todo else task_results.get()
                tasks_pending -= 1
                occupied.remove(worker)
                yield task.result()
            except queue.Empty:
                continue


    def _execute_onebyone(self, workers):
        for task in self.tasks:
            if task.preferred_workers:
                worker = task.preferred_workers[0]
            elif task.allowed_workers:
                worker = task.allowed_workers[0]
            else:
                worker = workers[0]
            future = task.execute(worker)
            yield future.result()


    def cancel(self):
        super().cancel()
        for task in self.tasks:
            task.cancel()


    def __lt__(self, other):
        assert self.id is not None
        assert self.job == other.job
        return self.id < other.id


    def __eq__(self, other):
        if other is None:
            return False
        assert self.id is not None
        assert self.job == other.job, '%s %s %s %s' % (self, self.job, other, other.job)
        return other and self.id == other.id

    def __repr__(self):
        return 'Stage(id=%s)' % self.id



@functools.total_ordering
class Task(Lifecycle, metaclass=abc.ABCMeta):

    def __init__(self, task_id, stage, method, args, kwargs,
                 preferred_workers=None, allowed_workers=None,
                 name=None, desc=None):
        super().__init__(name, desc)
        self.id = task_id
        self.stage = stage
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.preferred_workers = preferred_workers
        self.allowed_workers = allowed_workers
        self.future = None

    def execute(self, worker):
        self.signal_start()
        self.future = worker.run_task(self.method, *self.args, **(self.kwargs or {}))
        self.future.add_done_callback(lambda future: self.signal_stop())
        return self.future

    def result(self):
        assert self.future, 'task not yet scheduled'
        return self.future.result()

    def cancel(self):
        super().cancel()
        if self.future:
            self.future.cancel()


    def __lt__(self, other):
        assert self.stage == other.stage
        return self.id < other.id


    def __eq__(self, other):
        return other.id == self.id
