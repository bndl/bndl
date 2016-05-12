import logging
import threading

from bndl.rmi.node import RMINode


logger = logging.getLogger(__name__)


_current_worker = threading.local()
_current_worker_err_msg = '''\
Working outside of task context.

This typically means you attempted to use functionality that needs to interface
with the local worker node.
'''

def current_worker():
    try:
        return _current_worker.w
    except AttributeError:
        raise RuntimeError(_current_worker_err_msg)


class Worker(RMINode):
    def run_task(self, src, task, *args, **kwargs):
        logger.info('running task %s from %s', task, src.name)

        # set worker context
        _current_worker.w = self
        try:
            return task(self, *args, **kwargs)
        finally:
            # clean up worker context
            del _current_worker.w
