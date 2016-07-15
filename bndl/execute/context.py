from datetime import datetime, timedelta
import logging
import time

from bndl.execute.worker import current_worker
from bndl.util.lifecycle import Lifecycle
from bndl.util.conf import Config


logger = logging.getLogger(__name__)


class ExecutionContext(Lifecycle):

    def __init__(self, node, config=Config()):
        super().__init__()
        self._node = node
        self.conf = config
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
        if workers is None:
            self.await_workers()
            workers = self.workers[:]

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

            if min_connected_since < max(stable_time, timedelta(seconds=.5)):
                return False

            expected = self.worker_count ** 2 - self.worker_count
            tasks = [w.run_task(lambda w:sum(1 for w in w.peers.filter(node_type='worker') if w.is_connected)) for w in self.workers]
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


    def stop(self):
        self.signal_stop()


    def __getstate__(self):
        state = super().__getstate__()
        for attr in ('_node', 'jobs'):
            state.pop(attr, None)
        return state
