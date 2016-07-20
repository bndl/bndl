import logging

from bndl.execute.worker import Worker


logger = logging.getLogger(__name__)


class Driver(Worker):
    hosted_values = {}


    def get_hosted_value(self, src, key):
        logger.debug('sending broadcast value with key %s to %s', key, src)
        return self.hosted_values[key]


    def unpersist_broadcast_value(self, key):
        logger.debug('removing broadcast value with key %s to', key)
        if key in self.hosted_values:
            del self.hosted_values[key]

        futures = []
        for worker in self.peers.filter(node_type='worker'):
            try:
                futures.append(worker.unpersist_broadcast_value(key))
            except Exception:
                logger.debug('failed to unpersist broadcast value %r', key, exc_info=True)

        # wait for all to be done
        for future in futures:
            try:
                future.result()
            except Exception:
                logger.debug('failed to unpersist broadcast value %r', key, exc_info=True)
