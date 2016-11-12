from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
import logging
import time
import warnings

from bndl.execute.profile import CpuProfiling, MemoryProfiling
from bndl.execute.scheduler import Scheduler
from bndl.execute.worker import current_worker
from bndl.util import plugins
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)


def _num_connected():
    worker = current_worker()
    return sum(1 for w in worker.peers.filter(node_type='worker') if w.is_connected)


class ExecutionContext(Lifecycle):
    instances = set()

    def __init__(self, node, config=Config()):
        # Make sure the BNDL plugins are loaded
        plugins.load_plugins()

        super().__init__()
        self._node = node
        self.conf = config
        self.jobs = []
        self.signal_start()
        self.instances.add(self)


    @property
    def node(self):
        if self._node is None:
            with catch(RuntimeError):
                self._node = current_worker()
        return self._node


    def execute(self, job, workers=None, order_results=True, concurrency=None, attempts=None):
        assert self.running, 'context is not running'

        if workers is None:
            self.await_workers()
            workers = self.workers[:]

        if not job.tasks:
            return

        self.jobs.append(job)

        done = Queue()
        scheduler = Scheduler(self, job.tasks, done.put, workers, concurrency, attempts)
        scheduler_driver = Thread(target=scheduler.run,
                                  name='bndl-scheduler-%s' % (job.id),
                                  daemon=True)
        job.signal_start()
        scheduler_driver.start()

        try:
            if order_results:
                yield from self._execute_ordered(job, done)
            else:
                yield from self._execute_unordered(job, done)
            scheduler_driver.join()
        except KeyboardInterrupt:
            scheduler.abort()
            raise
        except GeneratorExit:
            scheduler.abort()
        finally:
            scheduler_driver.join()
            for task in job.tasks:
                task.release()
            job.signal_stop()


    def _execute_ordered(self, job, done):
        # keep a dict with results which are 'early' and the task id
        task_ids = iter(task.id for task in job.tasks)
        next_taskid = next(task_ids)
        early = {}

        for task in self._execute_unordered(job, done):
            if task.id != next_taskid:
                early[task.id] = task
            else:
                early.pop(task.id, None)
                yield task
                try:
                    # possibly yield tasks which are next
                    next_taskid = next(task_ids)
                    while early:
                        if next_taskid in early:
                            yield early.pop(next_taskid)
                            next_taskid = next(task_ids)
                        else:
                            break
                except StopIteration:
                    break

        assert not early, 'results for tasks %r not yet yielded' % early


    def _execute_unordered(self, job, done):
        seen = set()
        for task in iter(done.get, None):
            if isinstance(task, Exception):
                raise task
            elif task.failed:
                raise task.exception()
            elif task not in seen:
                seen.add(task)
                yield task


    def await_workers(self, worker_count=None, connect_timeout=5, stable_timeout=60):
        '''
        await_workers waits for workers to be available. If not in
        connect_timeout a RuntimeError is raised. Once a worker is found, at
        most stable_timeout seconds will be waited for the cluster to settle.
        That is, until no new workers are discovered / the worker count is
        stable.

        :param worker_count: int or None
            The expected worker count. When connected to exactly worker_count
            workers, this method will return faster.
        :param connect_timeout: int or float
            Maximum time in seconds waited until the first worker is discovered.
        :param stable_timeout: int or float
            Maximum time in seconds waited until no more workers are discovered.
        '''
        if not isinstance(connect_timeout, timedelta):
            connect_timeout = timedelta(seconds=connect_timeout)
        if not isinstance(stable_timeout, timedelta):
            stable_timeout = timedelta(seconds=stable_timeout)

        # workers are awaited in a loop, idling step_sleep each time
        # step_sleep increases until at most 1 second
        step_sleep = .1

        # remember when we started the wait
        wait_started = datetime.now()
        # and set deadlines for first connect and stability
        connected_deadline = wait_started + connect_timeout
        stable_deadline = wait_started + stable_timeout
        # for stability we don't look back further than the stable timeout
        stable_max_lookback = wait_started - stable_timeout

        def is_stable():
            '''
            Check whether the cluster of workers is 'stable'.

            The following heuristics are applied (assuming the default timeout
            values):

            - The cluster is considered stable if no connects are made in the
              last 60 seconds.
            - If there is only one such recent connects, at least 5 seconds
              has passed.
            - If there are more recent connects, the cluster is considered
              stable if at least twice the maximum interval between the
              connects has passed or 1 second, whichever is longer.
            '''
            assert self.worker_count > 0
            now = datetime.now()
            recent_connects = sorted(worker.connected_on for worker in self.workers
                                     if worker.connected_on > stable_max_lookback)
            if not recent_connects:
                return False, True
            elif len(recent_connects) == 1:
                return True, recent_connects[0] < now - connect_timeout
            else:
                max_connect_gap = max(b - a for a, b in zip(recent_connects, recent_connects[1:]))
                stable_time = max(2 * max_connect_gap, timedelta(seconds=1))
                time_since_last_connect = now - recent_connects[-1]
                return True, time_since_last_connect > stable_time

        def worker_count_consistent():
            '''Check if the workers all see each other'''
            expected = self.worker_count ** 2 - self.worker_count
            tasks = [(w, w.execute(_num_connected)) for w in self.workers]
            actual = 0
            for worker, task in tasks:
                try:
                    actual += task.result()
                except Exception:
                    logger.info("Couldn't get connected worker count from %r", worker, exc_info=True)
            return expected == actual

        while True:
            if worker_count == self.worker_count:
                return worker_count

            if self.worker_count == 0:
                if datetime.now() > connected_deadline:
                    raise RuntimeError('no workers available')
            else:
                recent_connects, stable = is_stable()

                if stable:
                    if not recent_connects:
                        return self.worker_count
                    elif recent_connects:
                        if worker_count_consistent():
                            return self.worker_count

                if datetime.now() > stable_deadline:
                    warnings.warn('Worker count not stable after %r' % stable_timeout)
                    return self.worker_count

                time.sleep(step_sleep)
                step_sleep = min(1, step_sleep * 1.5)


    @property
    def worker_count(self):
        return len(self.workers)


    @property
    def workers(self):
        return list(self.node.peers.filter(node_type='worker'))


    @property
    def cpu_profiling(self):
        return CpuProfiling(self)


    @property
    def memory_profiling(self):
        return MemoryProfiling(self)


    def stop(self):
        self.signal_stop()
        try:
            self.instances.remove(self)
        except KeyError:
            pass


    def __reduce__(self):
        return type(self), (None, self.conf)
