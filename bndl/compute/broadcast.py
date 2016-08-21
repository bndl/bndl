from uuid import uuid4
import logging

from bndl.util import serialize, threads
from bndl.util.conf import Float


block_size = Float(16)  # MB


logger = logging.getLogger(__name__)


MISSING = 'bndl.compute.broadcast.MISSING'


# TODO implement alternative encodings
# as with caching


# TODO move such 'caches' outside of modules
# much better to keep them local to Nodes
# if deleted, GC will kick in

download_coordinator = threads.Coordinator()


def broadcast(ctx, value):
    fmt, body = serialize.dumps(value)
    bcval = _broadcast(ctx, fmt, body)
    return bcval


def broadcast_pickled(ctx, pickled_value):
    return _broadcast(ctx, 0, pickled_value)


def _broadcast(ctx, fmt, body):
    data = bytearray()
    data.append(fmt)
    data.extend(body)
    key = str(uuid4())
    block_size = ctx.conf.get('bndl.compute.broadcast.block_size') * 1024 * 1024
    block_spec = ctx.node.serve_data(key, data, block_size)
    return BroadcastValue(ctx, ctx.node.name, block_spec)


class BroadcastValue(object):
    def __init__(self, ctx, seeder, block_spec):
        self.ctx = ctx
        self.seeder = seeder
        self.block_spec = block_spec


    @property
    def value(self):
        return download_coordinator.coordinate(self._get, self.block_spec.name)


    def _get(self):
        node = self.ctx.node
        blocks = node.get_blocks(self.block_spec)

        fmt = blocks[0][0]
        blocks[0] = memoryview(blocks[0])[1:]
        val = serialize.loads(fmt, b''.join(blocks))

        if node.name != self.block_spec.seeder:
            node.remove_blocks(self.block_spec.name, from_peers=False)

        return val


    def unpersist(self):
        node = self.ctx.node
        name = self.block_spec.name
        assert node.name == self.block_spec.seeder
        node.unpersist_broadcast_values(node, name)
        for peer in node.peers.filter():
            peer.unpersist_broadcast_values(name)
            # response isn't waited on


    def __del__(self):
        if self.ctx.node and self.ctx.node.name == self.block_spec.seeder:
            self.unpersist()
