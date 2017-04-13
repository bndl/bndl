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

import collections
import math

from bndl.compute.dataset import Dataset, Partition
from bndl.util import serialize
from bndl.util.collection import batch, ensure_collection, seqlen
from bndl.util.exceptions import catch


class DistributedCollection(Dataset):
    def __init__(self, ctx, collection, pcount=None, psize=None):
        super().__init__(ctx)

        if pcount is not None and pcount <= 0:
            raise ValueError('pcount must be None or > 0')
        if psize is not None and psize <= 0:
            raise ValueError('psize must be None or > 0')
        if pcount and psize:
            raise ValueError("can't set both pcount and psize")

        if psize:
            self.pcount = None
            self.psize = psize
        else:
            self.pcount = pcount
            self.psize = None

        if self.psize:
            parts = [(seqlen(part),) + serialize.dumps(part) for part in
                     map(ensure_collection, batch(collection, self.psize))]
            self.pcount = len(parts)
        else:
            if not pcount:
                self.pcount = pcount = self.ctx.default_pcount
                if pcount <= 0:
                    raise Exception("can't use default_pcount, no executors available")

            if isinstance(collection, collections.Mapping):
                collection = list(collection.items())
            elif not hasattr(collection, '__len__') or not hasattr(collection, '__getitem__'):
                collection = list(collection)

            step = max(1, math.ceil(seqlen(collection) / pcount))
            parts = [
                (seqlen(part),) + serialize.dumps(part) for part in
                (collection[idx * step: (idx + 1) * step] for idx in range(pcount))
                if seqlen(part)
            ]

        self.blocks = [
            (length, marshalled, ctx.node.service('blocks').serve_data((self.id, idx), part))
            for idx, (length, marshalled, part) in enumerate(parts)
        ]


    def parts(self):
        return [BlocksPartition(self, idx, block, marshalled, length)
                for idx, (length, marshalled, block) in enumerate(self.blocks)]


    def __getstate__(self):
        state = super().__getstate__()
        del state['blocks']
        return state


    def __del__(self):
        for block in getattr(self, 'blocks', ()):
            self.ctx.node.service('blocks').remove_block(block[-1].id)



class BlocksPartition(Partition):
    def __init__(self, dset, idx, block_spec, marshalled, length):
        super().__init__(dset, idx)
        self.block_spec = block_spec
        self.marshalled = marshalled
        self.length = length


    def _compute(self):
        blocks = self.dset.ctx.node.service('blocks')
        try:
            block = blocks.get(self.block_spec)
            with block.view() as v:
                return serialize.loads(self.marshalled, v)
        finally:
            with catch():
                blocks.remove_block(self.block_spec.id)
