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

from asyncio.futures import TimeoutError
from collections import defaultdict
from concurrent.futures import as_completed
from operator import itemgetter
import asyncio
import contextlib
import logging
import math
import random
import threading

from bndl.net.serialize import attach, attachment
from bndl.util.collection import split
from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)


AVAILABILITY_TIMEOUT = 1


class BlockSpec(object):
    def __init__(self, seeder, name, num_blocks):
        self.seeder = seeder
        self.name = name
        self.num_blocks = num_blocks


class Block(object):
    '''
    Helper class for exchanging blocks between peers. It uses the attach and
    attachment utilities from bndl.net.serialize to optimize sending the already
    serialized data.
    '''

    def __init__(self, block_id, data):
        self.id = block_id
        self.data = data


    def __getstate__(self):
        @contextlib.contextmanager
        def _attacher(loop, writer):
            @asyncio.coroutine
            def sender():
                writer.write(self.data)
            yield len(self.data), sender
        attach(str(self.id).encode(), _attacher)
        state = dict(self.__dict__)
        del state['data']
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self.data = attachment(str(self.id).encode())


def _batch_blocks(data, block_size):
    length = len(data)
    if isinstance(block_size, tuple):
        block_count, min_block_size, max_block_size = block_size
        assert block_count > 0
        assert min_block_size >= 0
        assert max_block_size > 0
        block_size = math.ceil(length / block_count)
        if block_size > max_block_size:
            block_size = max_block_size
        elif block_size < min_block_size:
            block_size = min_block_size
        return _batch_blocks(data, block_size)
    else:
        assert block_size > 0
        if length > block_size:
            # TODOdata = memoryview(data)
            num_blocks = int((length - 1) / block_size) + 1  # will be 1 short
            blocks = []
            step = math.ceil(length / num_blocks)
            offset = 0
            for _ in range(num_blocks - 1):
                blocks.append(data[offset:offset + step])
                offset += step
            # add remainder
            blocks.append(data[offset:])
            return blocks
        else:
            return [data]



class BlockManager(object):
    '''
    Block management functionality to be mixed in with RMINode.

    Use serve_data(name, data, block_size) or serve_blocks for hosting data (in
    blocks). Use remove_blocks(name, from_peers) to stop serving the blocks,
    remove the blocks from 'cache' and - if from_peers is True - also from
    peers nodes.

    Use get(block_spec) to access blocks. Note that this module does not
    provide a means to transmit block_spec. See e.g. bndl.compute.broadcast for
    usage of the BlockManager functionality.
    '''

    def __init__(self, worker):
        self.worker = worker
        # cache of blocks by name
        self.cache = {}  # name : lists
        # protects write access to _available_events
        self._available_lock = threading.Lock()
        # contains events indicating blocks are available (event is set)
        # or are being downloaded (contains name, but event not set)
        self._available_events = {}  # name : threading.Event


    def serve_data(self, name, data, block_size):
        '''
        Serve data from this node (it'll be the seeder). The method is a
        convenience method to split data into blocks.

        :param name: Name of the data / blocks.
        :param data: The bytes to be split into blocks.
        :param block_size: int or tuple
            If int: maximum size of the blocks.
            If tuple: ideal number of blocks, minimum and maximum size of the blocks.
        '''
        blocks = _batch_blocks(data, block_size)
        return self.serve_blocks(name, blocks)


    def serve_blocks(self, name, blocks):
        '''
        Serve blocks from this node (it'll be the seeder).
        :param name: Name of the blocks.
        :param blocks: list or tuple of blocks.
        '''
        block_spec = BlockSpec(self.worker.name, name, len(blocks))
        self.cache[name] = blocks
        available = threading.Event()
        self._available_events[name] = available
        available.set()
        return block_spec


    def remove_blocks(self, name, from_peers=False):
        '''
        Remove blocks being served.
        :param name: Name of the blocks to be removed.
        :param from_peers: If True, the blocks under name will be removed from
        other peer nodes as well.
        '''
        with catch(KeyError):
            del self.cache[name]
        with catch(KeyError):
            del self._available_events[name]
        if from_peers:
            for peer in self.worker.peers.filter():
                peer._remove_blocks(name)
                # responses aren't waited for


    @asyncio.coroutine
    def _remove_blocks(self, peer, name):
        with catch(KeyError):
            del self.cache[name]
        with catch(KeyError):
            del self._available_events[name]


    def get(self, block_spec, peers=[]):
        name = block_spec.name
        with self._available_lock:
            available = self._available_events.get(name)
            if available:
                in_progress = True
            else:
                in_progress = False
                available = threading.Event()
                self._available_events[name] = available
        if in_progress:
            available.wait()
        else:
            self._download(block_spec, peers)
            available.set()
        return self.cache[name]


    @asyncio.coroutine
    def _get(self, peer, name, idx):
        logger.debug('sending block %s of %s to %s', idx, name, peer.name)
        return Block(idx, self.cache[name][idx])


    @asyncio.coroutine
    def _get_available(self, peer, name):
        try:
            blocks = self.cache[name]
        except KeyError:
            return ()
        else:
            return [idx for idx, block in enumerate(blocks) if block is not None]


    def _next_download(self, block_spec, peers):
        # check block availability at peers
        available_requests = []
        for peer in peers:
            available_request = peer.service('blocks')._get_available(block_spec.name)
            available_request.peer = peer
            available_requests.append(available_request)

        blocks = self.cache[block_spec.name]

        # store peers which have block (keyed by block index)
        availability = defaultdict(list)
        try:
            for available_request in as_completed(available_requests, timeout=AVAILABILITY_TIMEOUT):
                try:
                    for idx in available_request.result():
                        if blocks[idx] is None:
                            availability[idx].append(available_request.peer)
                except Exception:
                    logger.warning('Unable to get block availability from %s', available_request.peer, exc_info=True)
        except TimeoutError:
            # raised from as_completed(...).__next__() if the next
            pass

        # if any blocks available, select the one with the highest availability
        if availability:
            return sorted(availability.items(), key=itemgetter(1), reverse=True)[0]
        else:
            remaining = [idx for idx, block in enumerate(blocks) if block is None]
            block_idx = random.choice(remaining)
            return block_idx, [self.worker.peers[block_spec.seeder]]


    def _download(self, block_spec, peers):
        name = block_spec.name
        assert block_spec.seeder != self.worker.name

        blocks = [None] * block_spec.num_blocks
        self.cache[name] = blocks

        local_ips = self.worker.ip_addresses()

        for _ in blocks:
            idx, candidates = self._next_download(block_spec, peers)
            candidates = split(candidates, lambda c: bool(c.ip_addresses() & local_ips))
            local, remote = candidates[True], candidates[False]

            def download(source):
                blocks[idx] = source.service('blocks')._get(name, idx).result().data

            while local or remote:
                # select a random candidate, preferring local ones
                candidates = local or remote
                source = random.choice(candidates)
                candidates.remove(source)
                try:
                    download(source)
                except Exception:
                    pass  # availability info may have been stale, node may be gone, ...
                else:
                    break  # break after download
            else:
                # no non-seeder candidates, or all failed
                # fall back to downloading from seeder
                download(self.peers[block_spec.seeder])
