import logging

from bndl.execute.worker import Worker


logger = logging.getLogger(__name__)


class Driver(Worker):
    broadcast_values = {}


    def get_broadcast_value(self, src, key):
        logger.debug('sending broadcast value with key %s to %s', key, src)
        return self.broadcast_values[key]


    def unpersist_broadcast_value(self, key):
        logger.debug('removing broadcast value with key %s to', key)
        if key in self.broadcast_values:
            del self.broadcast_values[key]
        # TODO better error handling
        futures = []
        for worker in self.peers.filter(node_type='worker'):
            try:
                futures.append(worker.unpersist_broadcast_value(key))
            except Exception:
                pass
        # wait for all to be done
        [future.result() for future in futures]
