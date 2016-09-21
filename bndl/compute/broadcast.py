from uuid import uuid4
import json
import logging
import marshal
import pickle

from bndl.util import serialize, threads
from bndl.util.conf import Float
from bndl.util.funcs import identity
from cytoolz.functoolz import compose


min_block_size = Float(4)  # MB
max_block_size = Float(16)  # MB


logger = logging.getLogger(__name__)


MISSING = 'bndl.compute.broadcast.MISSING'


# TODO implement alternative encodings
# as with caching


# TODO move such 'caches' outside of modules
# much better to keep them local to Nodes
# if deleted, GC will kick in

download_coordinator = threads.Coordinator()


def broadcast(ctx, value, serialization='auto', deserialization=None):
    '''
    Broadcast data to workers
    :param value: The value to broadcast
    :param serialization: auto, pickle, marshal, json, binary or text
        The format to serialize the broadcast value into
    :param deserialization: None or function(fileobj)
    '''
    if serialization is not None:
        if deserialization is not None:
            raise ValueError("Can't specify both serialization and deserialization")
        elif serialization == 'auto':
            marshalled, data = serialize.dumps(value)
            deserialization = marshal.loads if marshalled else pickle.loads
        elif serialization == 'pickle':
            data = pickle.dumps(value)
            deserialization = pickle.loads
        elif serialization == 'marshal':
            data = marshal.dumps(value)
            deserialization = marshal.loads
        elif serialization == 'json':
            data = json.dumps(value).encode()
            deserialization = compose(json.loads, bytes.decode)
        elif serialization == 'binary':
            data = value
            deserialization = identity
        elif serialization == 'text':
            data = value.encode()
            deserialization = bytes.decode
        else:
            raise ValueError('Unsupported serialization %s' % serialization)
    elif not deserialization:
        raise ValueError('Must specify either serialization or deserialization')
    else:
        data = value

    key = str(uuid4())
    min_block_size = int(ctx.conf.get('bndl.compute.broadcast.min_block_size') * 1024 * 1024)
    max_block_size = int(ctx.conf.get('bndl.compute.broadcast.max_block_size') * 1024 * 1024)
    if min_block_size == max_block_size:
        block_spec = ctx.node.serve_data(key, data, max_block_size)
    else:
        block_spec = ctx.node.serve_data(key, data, (ctx.worker_count * 2, min_block_size, max_block_size))
    return BroadcastValue(ctx, ctx.node.name, block_spec, deserialization)


def broadcast_pickled(ctx, pickled_value):
    '''
    Broadcast data which is already pickled.
    '''
    return broadcast(ctx, pickled_value, serialization=None, deserialization=pickle.loads)


class BroadcastValue(object):
    def __init__(self, ctx, seeder, block_spec, deserialize):
        self.ctx = ctx
        self.seeder = seeder
        self.block_spec = block_spec
        self.deserialize = deserialize


    @property
    def value(self):
        return download_coordinator.coordinate(self._get, self.block_spec.name)


    def _get(self):
        node = self.ctx.node
        blocks = node.get_blocks(self.block_spec, node.peers.filter(node_type='worker'))

        val = self.deserialize(b''.join(blocks))

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
