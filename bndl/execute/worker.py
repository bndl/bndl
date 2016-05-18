import logging
import threading

from bndl.rmi.node import RMINode


logger = logging.getLogger(__name__)


_CURRENT_WORKER = threading.local()
_CURRENT_WORKER_ERR_MSG = '''\
Working outside of task context.

This typically means you attempted to use functionality that needs to interface
with the local worker node.
'''


def current_worker():
    try:
        return _CURRENT_WORKER.w
    except AttributeError:
        raise RuntimeError(_CURRENT_WORKER_ERR_MSG)


class Worker(RMINode):
    def run_task(self, src, task, *args, **kwargs):
        logger.info('running task %s from %s', task, src.name)

        # set worker context
        _CURRENT_WORKER.w = self
        try:
            return task(self, *args, **kwargs)
        finally:
            # clean up worker context
            del _CURRENT_WORKER.w
