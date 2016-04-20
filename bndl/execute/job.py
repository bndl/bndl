import abc
import concurrent.futures
import functools
import logging

from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)


class Job(Lifecycle):

    def __init__(self, ctx):
        super().__init__()
        self.ctx = ctx
        self.stages = []


    def execute(self, eager=True):
        self._signal_start()

        logger.info('executing job %s with stages %s', self, self.stages)

        for stage in self.stages:
            if not self.running:
                break
            logger.info('executing stage %s with %s tasks', stage, len(stage.tasks))
            stage.add_listener(self._stage_done)
            yield stage.execute(eager=eager)


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


    def execute(self, eager=True):
        self._signal_start()

        # TODO run one task per worker at most?
        # throttle here? or at the worker? or both?
        # having a second task in flight keeps throughput up ...
        executed = (task.execute() for task in self.tasks)

        # materializing the generator above into a list forces the immediate
        # (asynchronous) execution of self.tasks
        if eager:
            executed = list(executed)

        # TODO timeout and reschedule

        try:
            exc = None

            for future in executed:
                if exc:
                    future.cancel()
                else:
                    try:
                        yield future.result()
                    except Exception as e:
                        # TODO reschedule?
                        # unless CancelledError of course
                        # cancel job after x retries on different machines?
                        exc = e
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

    def execute(self):
        # TODO reschedule on failure / timeout / ...
        self._signal_start()

        workers = self.preferred_workers + (self.allowed_workers or self.stage.job.ctx.workers)
        for worker in workers:
            if worker.is_connected:
                break
        self.future = worker.run_task(self.method, *self.args, **(self.kwargs or {}))
        self.future.add_done_callback(lambda future: self._signal_stop())
        return self.future


    def cancel(self):
        super().cancel()
        if self.future:
            self.future.cancel()


    def __lt__(self, other):
        assert self.stage == other.stage
        return self.id < other.id


    def __eq__(self, other):
        return other.id == self.id
