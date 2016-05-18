from datetime import datetime
import logging
import time

from bndl.execute.worker import current_worker
from bndl.util.lifecycle import Lifecycle
from bndl.util.conf import Config


logger = logging.getLogger(__name__)


class ExecutionContext(Lifecycle):

    def __init__(self, driver, conf={}):
        super().__init__()
        self._driver = driver
        self._node = driver
        self.conf = Config(conf)
        self.jobs = []
        self.signal_start()

    @property
    def node(self):
        node = getattr(self, '_node', None)
        if node:
            return node
        else:
            self._node = current_worker()
            return self._node

    def execute(self, job, workers=None, eager=True):
        # TODO what if not everything is consumed?
        assert self.running, 'context is not running'
        self.await_workers()
        workers = workers or self.workers[:]

        try:
            for listener in self.listeners:
                job.add_listener(listener)

            self.jobs.append(job)
            execution = job.execute(workers=workers, eager=eager)
            for stage, stage_execution in zip(job.stages, execution):
                for result in stage_execution:
                    if stage == job.stages[-1]:
                        yield result
        finally:
            for listener in self.listeners:
                job.remove_listener(listener)


    def await_workers(self, connect_timeout=5, stable_timeout=60):
        '''
        await_workers waits for workers to be available. If not in
        connect_timeout a RuntimeError is raised. Once a worker is found, at
        most stable_timeout seconds will be waited for the cluster to settle.
        That is, until no new workers are discovered / the worker count is
        stable.

        :param connect_timeout: int or float
            Maximum time in seconds waited until the first worker is discovered.
        :param stable_timeout: int or float
            Maximum time in seconds waited until no more workers are discovered.
        '''
        step_sleep = .1

        if self.workers and (datetime.now() - max(worker.connected_on for worker in self.workers)).total_seconds() > step_sleep:
            return

        # wait connect_timeout seconds to find first worker
        for _ in range(int(connect_timeout // step_sleep)):
            if self.workers:
                break
            time.sleep(step_sleep)
        if not self.workers:
            raise RuntimeError('no workers available')

        # wait stable_timeout to let the discovery complete
        for _ in range(int(stable_timeout // step_sleep)):
            count = self.worker_count
            time.sleep(step_sleep)
            if self.worker_count == count:
                break
            step_sleep = min(step_sleep * 3, 5)


    @property
    def worker_count(self):
        return len(self.workers)


    @property
    def workers(self):
        return list(self.node.peers.filter(node_type='worker'))


    def stop(self):
        self.signal_stop()


    def __getstate__(self):
        state = super().__getstate__()
        for attr in ('_driver', '_node', 'jobs'):
            state.pop(attr, None)
        return state
