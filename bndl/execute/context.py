import time
import logging
from bndl.util.lifecycle import Lifecycle
from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)


class ExecutionContext(Lifecycle):

    def __init__(self, driver, conf=None):
        super().__init__()
        self._driver = driver
        self.node = driver
        self.conf = conf
        self.jobs = []
        self.signal_start()


    def execute(self, job, workers=None, eager=True):
        # TODO what if not everything is consumed?
        assert self.running, 'context is not running'
        if not workers or not self.workers:
            self._await_workers()
        workers = workers or self.workers[:]

        try:
            for l in self.listeners:
                job.add_listener(l)

            self.jobs.append(job)
            execution = job.execute(workers=workers, eager=eager)
            for stage, stage_execution in zip(job.stages, execution):
                for result in stage_execution:
                    if stage == job.stages[-1]:
                        yield result
        finally:
            for l in self.listeners:
                job.remove_listener(l)


    def _await_workers(self, timeout=1):
        if not self.workers:
            step_sleep = .01
            for _ in range(int(1 // step_sleep)):
                time.sleep(step_sleep)
                if self.workers:
                    return
        if not self.workers:
            raise Exception('no workers available')


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
        for attr in ('_driver', 'node', 'jobs'):
            state.pop(attr, None)
        return state
