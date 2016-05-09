from uuid import uuid4
from bndl.util import serialize
import logging


logger = logging.getLogger(__name__)


MISSING = 'bndl.broadcast.MISSING'


def broadcast(ctx, value):
    driver = ctx.node
    key = str(uuid4())
    driver.broadcast_values[key] = serialize.dumps(value)
    return BroadcastValue(ctx, key, value)

def broadcast_pickled(ctx, pickled_value):
    driver = ctx.node
    key = str(uuid4())
    driver.broadcast_values[key] = False, pickled_value
    return BroadcastValue(ctx, key)


class BroadcastValue(object):
    def __init__(self, ctx, key, value=MISSING):
        self.ctx = ctx
        self.key = key
        self._value = value

    def __getstate__(self):
        return dict(ctx=self.ctx, key=self.key)


    @property
    def value(self):
        if not getattr(self, '_value', MISSING) == MISSING:
            return self._value
        node = self.ctx.node
        v = node.broadcast_values.get(self.key, MISSING)
        if v == MISSING:
            driver = node.peers.filter(node_type='driver')[0]
            logger.debug('retrieving broadcast value with key %s from %s', self.key, driver)
            marshalled, payload = driver.get_broadcast_value(self.key).result()
            v = serialize.loads(marshalled, payload)
            node.broadcast_values[self.key] = v
            self._value = v
        return v


    def unpersist(self):
        assert self.ctx.node.node_type == 'driver'
        self.ctx.node.unpersist_broadcast_value(self.key)
