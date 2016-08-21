import logging

from bndl.compute.worker import Worker


logger = logging.getLogger(__name__)


class Driver(Worker):
    hosted_values = {}


    def get_hosted_value(self, src, key):
        logger.debug('sending broadcast value with key %s to %s', key, src)
        return self.hosted_values[key]
