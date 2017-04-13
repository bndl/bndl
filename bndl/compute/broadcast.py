# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import concurrent.futures
import json
import logging
import marshal
import pickle

from cytoolz import compose

from bndl.net import rmi
from bndl.util import serialize, threads, strings
from bndl.util.funcs import identity
from bndl.util.strings import decode


logger = logging.getLogger(__name__)
download_coordinator = threads.Coordinator()


MISSING = 'bndl.compute.broadcast.MISSING'



class BroadcastManager(object):
    def __init__(self, node):
        self.node = node
        self.cache = {}


    @rmi.direct
    def drop(self, src, val_id):
        self.cache.pop(val_id, None)
        self.node.service('blocks')._remove_block(src, val_id)



def broadcast(ctx, value, serialization='auto', deserialization=None):
    '''
    Broadcast data to nodes.

    Args:

        value (object): The value to broadcast.
        serialization (str): The format to serialize the broadcast value into. Must be one of auto,
            pickle, marshal, json, binary or text.
        deserialization (None or function(bytes)):

    Data can be 'shipped' along to nodes in the closure of e.g. a mapper function, but in that
    case the data is sent once for every partition (task to be precise). For 'larger' values this
    may be wasteful. Use this for instance with lookup tables of e.g. a MB or more.

    Note that the broadcast data is loaded on each node (but only if the broadcast variable is
    used). The machine running the nodes should thus have enough memory to spare.

    If deserialization is set serialization must *not* be set and value must be of type `bytes`.
    Otherwise serialize is used to serialize value and its natural deserialization counterpart is
    used (e.g. bytes.decode followed by json.loads for the 'json' serialization format).

    Example usage::

        >>> tbl = ctx.broadcast(dict(zip(range(4), 'abcd')))
        >>> ctx.range(4).map(lambda i: tbl.value[i]).collect()
        ['a', 'b', 'c', 'd']

    '''
    if deserialization is not None:
        if serialization not in ('auto', None):
            raise ValueError("Can't specify both serialization and deserialization")
        if not isinstance(value, (bytes, memoryview, bytearray)):
            raise ValueError('Value must be bytes, memoryview or bytearray if deserialization is'
                             ' set, not %r', type(value))
        data = value

    elif serialization is not None:
        if serialization == 'auto':
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
            deserialization = compose(json.loads, decode)
        elif serialization == 'binary':
            data = value
            deserialization = identity
        elif serialization == 'text':
            data = value.encode()
            deserialization = decode
        else:
            raise ValueError('Unsupported serialization %s' % serialization)

    else:
        raise ValueError('Must specify either serialization or deserialization')

    key = ('broadcast', strings.random(8))
    block_spec = ctx.node.service('blocks').serve_data(key, data)
    return BroadcastValue(ctx, ctx.node.name, block_spec, deserialization)



class BroadcastValue(object):
    def __init__(self, ctx, seeder, block_spec, deserialize):
        self.ctx = ctx
        self.seeder = seeder
        self.block_spec = block_spec
        self.deserialize = deserialize


    @property
    def value(self):
        return download_coordinator.coordinate(self._get, self.block_spec.id)


    def _get(self):
        cache = self.ctx.node.service('broadcast').cache
        if self.block_spec.id in cache:
            return cache[self.block_spec.id]

        block = self.ctx.node.service('blocks').get(self.block_spec)
        with block.view() as v:
            bc_val = self.deserialize(v)
        del block

        cache[self.block_spec.id] = bc_val
        return bc_val


    def unpersist(self, block=False, timeout=None):
        node = self.ctx.node
        block_id = self.block_spec.id
        assert node.name == self.block_spec.seeder

        node.service('broadcast').drop(node, block_id)

        requests = []
        for peer in node.peers.filter():
            request = peer.service('broadcast').drop
            if timeout:
                request = request.with_timeout(timeout)
            requests.append(request(block_id))

        if block:
            for request in requests:
                try:
                    request.result()
                except concurrent.futures.TimeoutError:
                    pass
                except Exception:
                    logger.warning('error while unpersisting %s', block_id, exc_info=True)


    def __del__(self):
        if self.ctx.node and self.ctx.node.name == self.block_spec.seeder:
            self.unpersist()
