import time
import logging
from bndl.util.lifecycle import Lifecycle


logger = logging.getLogger(__name__)


class ExecutionContext(Lifecycle):

    def __init__(self, driver, conf=None):
        super().__init__()
        self._driver = driver
        self.node = driver
        self._signal_start()
        self.conf = conf


    def execute(self, job, eager=True):
        # TODO what if not everything is consumed?
        assert self.running, 'context is not running'
        self._await_workers()

        try:
            for l in self._listeners:
                job.add_listener(l)

            for stage, stage_execution in zip(job.stages, job.execute(eager=True)):
                for result in stage_execution:
                    if stage == job.stages[-1]:
                        yield result
        finally:
            for l in self._listeners:
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
        self._signal_stop()


    def __getstate__(self):
        state = super().__getstate__()
        for attr in ('_driver', 'node'):
            state.pop(attr, None)
        return state
