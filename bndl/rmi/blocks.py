from collections import defaultdict
from concurrent.futures._base import as_completed
from operator import itemgetter
import logging
import math
import random
import threading

from bndl.util.collection import split
from bndl.util.exceptions import catch
from asyncio.futures import TimeoutError


logger = logging.getLogger(__name__)


class BlockSpec(object):
    def __init__(self, seeder, name, num_blocks):
        self.seeder = seeder
        self.name = name
        self.num_blocks = num_blocks


AVAILABILITY_TIMEOUT = 1


class BlockManager:
    '''
    Block management functionality to be mixed in with RMINode
    '''

    def __init__(self):
        # cache of blocks by name
        self._blocks_cache = {}  # name : lists
        # protects write access to _available_events
        self._available_lock = threading.Lock()
        # contains events indicating blocks are available (event is set)
        # or are being downloaded (contains name, but event not set)
        self._available_events = {}  # name : threading.Event


    def serve_data(self, name, data, block_size=None):
        assert block_size > 0
        length = len(data)
        if length > block_size:
            blocks = []
            parts = int((length - 1) / block_size)  # will be 1 short
            step = math.ceil(length / (parts + 1))
            offset = 0
            for _ in range(parts):
                blocks.append(data[offset:offset + step])
                offset += step
            # add remainder
            blocks.append(data[offset:])
        else:
            blocks = [data]
        return self.serve_blocks(name, blocks)


    def serve_blocks(self, name, blocks):
        block_spec = BlockSpec(self.name, name, len(blocks))
        self._blocks_cache[name] = blocks
        available = threading.Event()
        self._available_events[name] = available
        available.set()
        return block_spec


    def remove_blocks(self, name, from_peers=True):
        self._remove_blocks(self, name)
        if from_peers:
            for peer in self.peers.filter():
                peer._remove_blocks(name)
                # responses aren't waited for


    def _remove_blocks(self, peer, name):
        with catch(KeyError):
            del self._blocks_cache[name]
        with catch(KeyError):
            del self._available_events[name]


    def get_blocks(self, block_spec):
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
            self._download(block_spec)
            available.set()
        return self._blocks_cache[name]


    def _get_block(self, peer, name, idx):
        logger.debug('sending block %s of %s to %s', idx, name, peer.name)
        # TODO optimise with pickle attachment protocol
        return self._blocks_cache[name][idx]


    def _get_blocks_available(self, peer, name):
        try:
            blocks = self._blocks_cache[name]
        except KeyError:
            return ()
        else:
            return [idx for idx, block in enumerate(blocks) if block is not None]


    def _next_download(self, block_spec):
        # check block availability at peers
        available_requests = []
        for worker in self.peers.filter(node_type='worker'):
            available_request = worker._get_blocks_available(block_spec.name)
            available_request.worker = worker
            available_requests.append(available_request)

        blocks = self._blocks_cache[block_spec.name]

        # store workers which have block (keyed by block index)
        availability = defaultdict(list)
        try:
            for available_request in as_completed(available_requests, timeout=AVAILABILITY_TIMEOUT):
                try:
                    for idx in available_request.result():
                        if blocks[idx] is None:
                            availability[idx].append(available_request.worker)
                except Exception:
                    logger.warning('Unable to get block availability from %s', available_request.worker, exc_info=True)
        except TimeoutError:
            # raised from as_completed(...).__next__() if the next
            pass

        # if any blocks available, select the one with the highest availability
        if availability:
            return sorted(availability.items(), key=itemgetter(1), reverse=True)[0]
        else:
            remaining = [idx for idx, block in enumerate(blocks) if block is None]
            block_idx = random.choice(remaining)
            return block_idx, [self.peers[block_spec.seeder]]


    def _download(self, block_spec):
        name = block_spec.name
        assert block_spec.seeder != self.name

        blocks = [None] * block_spec.num_blocks
        self._blocks_cache[name] = blocks

        local_ips = self.ip_addresses

        for _ in blocks:
            idx, candidates = self._next_download(block_spec)
            candidates = split(candidates, lambda c: bool(c.ip_addresses & local_ips))
            local, remote = candidates[True], candidates[False]
            
            def download(source):
                blocks[idx] = source._get_block(name, idx).result()
            
            while local or remote:
                # select a random candidate, preferring local ones
                candidates = local or remote
                source = random.choice(candidates)
                candidates.remove(source)
                try:
                    download(source)
                except Exception:
                    pass # availability info may have been stale, node may be gone, ...
                else:
                    break # break after dowload
            else:
                # fall back to downloading from seeder
                download(self.peers[block_spec.seeder])
