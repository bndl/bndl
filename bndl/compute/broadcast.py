from uuid import uuid4


MISSING = 'bndl.broadcast.MISSING'


def broadcast(ctx, value):
    driver = ctx.node
    key = str(uuid4())
    driver.broadcast_values[key] = value
    return BroadcastValue(ctx, key)


class BroadcastValue(object):
    def __init__(self, ctx, key):
        self.ctx = ctx
        self.key = key

    @property
    def value(self):
        node = self.ctx.node
        if node.node_type == 'worker':
            return node.get_broadcast_value(self.key)
        else:
            return node.broadcast_values[self.key]


    def unpersist(self):
        assert self.ctx.node.node_type == 'driver'
        self.ctx.node.unpersist_broadcast_value(self.key)
