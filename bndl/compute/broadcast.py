import logging
from uuid import uuid4

from bndl.util import serialize


logger = logging.getLogger(__name__)


MISSING = 'bndl.broadcast.MISSING'


def broadcast(ctx, value):
    bcval = BroadcastValue(ctx, value)
    ctx.node.hosted_values[bcval.key] = serialize.dumps(value)
    return bcval


def broadcast_pickled(ctx, pickled_value):
    bcval = BroadcastValue(ctx)
    ctx.node.hosted_values[bcval.key] = False, pickled_value
    return bcval



class BroadcastValue(object):
    def __init__(self, ctx, value=MISSING):
        self.ctx = ctx
        self._value = value
        self.key = str(uuid4())


    def __getstate__(self):
        return dict(ctx=self.ctx, key=self.key)


    @property
    def value(self):
        if getattr(self, '_value', MISSING) != MISSING:
            return self._value

        node = self.ctx.node
        val = node.broadcast_values_cache.get(self.key, MISSING)

        if val == MISSING:
            driver = node.peers.filter(node_type='driver')[0]
            logger.debug('retrieving broadcast value with key %s from %s', self.key, driver)
            marshalled, payload = driver.get_hosted_value(self.key).result()
            val = serialize.loads(marshalled, payload)
            node.broadcast_values_cache[self.key] = val
            self._value = val

        return val


    def unpersist(self):
        assert self.ctx.node.node_type == 'driver'
        self.ctx.node.unpersist_broadcast_value(self.key)
