import logging

from bndl.rmi.node import RMINode


logger = logging.getLogger(__name__)


class Worker(RMINode):
    def run_task(self, src, task, *args, **kwargs):
        logger.info('running task %s from %s', task, src.name)
        return task(self, *args, **kwargs)
