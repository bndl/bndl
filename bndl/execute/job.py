import abc
import functools
from itertools import chain
import logging
import queue

import concurrent.futures

from bndl.util.lifecycle import Lifecycle
import itertools


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

        available_workers = queue.Queue()
        for worker in workers:
            available_workers.put(worker)

        tasks_todo = self.tasks[:]
        task_results = queue.Queue()

        exc = None
        stop = False

        def start_next_task(available_worker=None):
            '''
            :param done: A worker done with it's previous task
            '''
            while not stop:
                try:
                    task = tasks_todo.pop()
                except IndexError:
                    return

                if eager and not available_worker:
                    try:
                        available_worker = available_workers.get()
                    except queue.Empty:
                        return

                # check if available_worker is a suitable worker
                if task.preferred_workers or task.allowed_workers:
                    for worker in chain.from_iterable((task.preferred_workers or (), task.allowed_workers)):
                        if worker == available_worker:
                            break
                else:
                    worker = available_worker


                if worker:
                    # start the task if suitable
                    future = task.execute(worker)
                    future.add_done_callback(lambda future: start_next_task(worker))
                    task_results.put(task)
                    return
                else:
                    # or put it back on the todo list
                    tasks_todo.insert(0, task)
                    available_workers.put(available_worker)

        try:
            if eager:
                # start a task for each worker if eager
                for _ in workers:
                    start_next_task(available_workers.get())
            else:
                # or just go about it one at a time
                start_next_task(available_workers.get())

            # complete len(self.tasks)
            for _ in range(len(self.tasks)):
                task = task_results.get()
                try:
                    yield task.result()
                except Exception as e:
                    # TODO retry on other worker
                    stop = True
                    exc = e
                    break

            if stop:
                for task in self.tasks:
                    task.cancel()

            # raise any error
            if exc:
                raise exc

        except concurrent.futures.CancelledError as e:
            return
        except Exception as e:
            root = e
            while root.__cause__:
                root = root.__cause__
            raise Exception('Unable to execute stage %s: %s' % (self, root)) from e
        finally:
            self.signal_stop()


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
