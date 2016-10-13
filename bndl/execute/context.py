from datetime import datetime, timedelta
import logging
import time
import weakref

from bndl.execute.profile import CpuProfiling, MemoryProfiling
from bndl.execute.worker import current_worker
from bndl.util import plugins
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)


def _num_connected(worker):
    return sum(1 for worker
               in worker.peers.filter(node_type='worker')
               if worker.is_connected)


class ExecutionContext(Lifecycle):
    instances = weakref.WeakSet()

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
            with catch():
                self._node = current_worker()
        return self._node


    def execute(self, job, workers=None, eager=True, ordered=True):
        # TODO what if not everything is consumed?
        assert self.running, 'context is not running'
        if workers is None:
            self.await_workers()
            workers = self.workers[:]

        concurrency = self.conf.get('bndl.execute.concurrency')

        try:
            for listener in self.listeners:
                job.add_listener(listener)

            self.jobs.append(job)
            execution = job.execute(workers, eager, ordered, concurrency)
            for stage, stage_execution in zip(job.stages, execution):
                for result in stage_execution:
                    if stage == job.stages[-1]:
                        yield result
        finally:
            for listener in self.listeners:
                job.remove_listener(listener)


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

        step_sleep = .1
        wait_started = datetime.now()

        def connections_stable():
            count = self.worker_count
            # not 'stable' until at least one worker found
            if count == 0:
                return False

            if count == worker_count:
                return True

            # calculate a sorted list of when workers have connected
            connected_on = sorted(worker.connected_on for worker in self.workers)
            min_connected_since = datetime.now() - connected_on[-1]

            if min_connected_since > stable_timeout:
                return True

            if count == 1:
                # if only one worker found, wait at least connect_timeout seconds
                stable_time = connect_timeout
            else:
                # otherwise, find out the max time between connects
                # and wait twice as long
                stable_time = max(b - a for a, b in zip(connected_on, connected_on[1:])) * 2
                # but for no longer than connect_timeout
                stable_time = min(connect_timeout, stable_time)

            if min_connected_since < max(stable_time, timedelta(seconds=.5)):
                return False

            expected = self.worker_count ** 2 - self.worker_count
            tasks = [w.run_task(_num_connected) for w in self.workers]
            actual = sum(t.result() for t in tasks)

            return expected == actual

        if self.workers and connections_stable():
            return self.worker_count

        # wait connect_timeout to find first worker
        while datetime.now() - wait_started < connect_timeout:
            if self.workers:
                break
            time.sleep(step_sleep)
        if not self.workers:
            raise RuntimeError('no workers available')

        # wait stable_timeout to let the discovery complete
        while datetime.now() - wait_started < stable_timeout:
            if connections_stable():
                break
            time.sleep(step_sleep)
            if step_sleep < 1:
                step_sleep *= 2

        return self.worker_count


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


    def __getstate__(self):
        state = super().__getstate__()
        for attr in ('_node', 'jobs'):
            state[attr] = None
        return state
