import abc
import functools
from itertools import chain
import logging
import queue

import concurrent.futures

from bndl.util.lifecycle import Lifecycle
from threading import Lock
from operator import setitem


logger = logging.getLogger(__name__)


class Job(Lifecycle):

    def __init__(self, ctx):
        super().__init__()
        self.ctx = ctx
        self.stages = []


    def execute(self, eager=True):
        self._signal_start()

        logger.info('executing job %s with stages %s', self, self.stages)

        workers = self.ctx.workers[:]

        for stage in self.stages:
            if not self.running:
                break
            logger.info('executing stage %s with %s tasks', stage, len(stage.tasks))
            stage.add_listener(self._stage_done)
            yield stage.execute(workers, eager)


    def _stage_done(self, stage):
        if stage.stopped and stage == self.stages[-1]:
            self._signal_stop()


    def cancel(self):
        super().cancel()
        for stage in self.stages:
            stage.cancel()



@functools.total_ordering
class Stage(Lifecycle):

    def __init__(self, stage_id, job):
        super().__init__()
        self.id = stage_id
        self.job = job
        self.tasks = []

    def execute(self, workers, eager=True):
        self._signal_start()

        todo = self.tasks[:]
        running = {w:None for w in workers}
        assignment_lock = Lock()
        results = queue.Queue()

        exc = None
        stop = False

        def select_worker(task):
            for worker in chain.from_iterable((task.preferred_workers or (), task.allowed_workers or workers)):
                if worker in workers and worker.is_connected and not running[worker]:
                    return worker

        def start_next_task(done=None):
            if done:
                worker, task = done
                running[worker] = None
            if stop:
                return
            for idx, task in enumerate(todo[:]):
                with assignment_lock:
                    w = select_worker(task)
                    if w:
                        running[w] = task
                        del todo[idx]
                        future = task.execute(w)
                        future.add_done_callback(lambda future: start_next_task((w, task)))
                        results.put(task)
                        break
        try:
            if eager:
                # start a task for each worker if eager
                for _ in workers:
                    start_next_task()
            else:
                # or just go about it one at a time
                start_next_task()

            # complete len(self.tasks)
            for _ in range(len(self.tasks)):
                task = results.get()
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
            self._signal_stop()


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
                 preferred_workers, allowed_workers=None):
        super().__init__()
        self.id = task_id
        self.stage = stage
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.preferred_workers = preferred_workers
        self.allowed_workers = allowed_workers
        self.future = None

    def execute(self, worker):
        self._signal_start()
        self.future = worker.run_task(self.method, *self.args, **(self.kwargs or {}))
        self.future.add_done_callback(lambda future: self._signal_stop())
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
