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

from bisect import bisect_left
from math import ceil
import asyncio
import logging
import random
import threading

from bndl.compute.storage import SharedMemoryData, InMemoryData
from bndl.net import rmi
from bndl.net.aio import run_coroutine_threadsafe
from bndl.util.conf import Float
from bndl.util.exceptions import catch
import bndl


logger = logging.getLogger(__name__)


AVAILABILITY_TIMEOUT = 1


min_download_size_mb = Float(1, desc='Minimum chunk size downloaded in megabytes')
max_download_size_mb = Float(8, desc='Maximum chunk size downloaded in megabytes')



class BlockSpec(object):
    def __init__(self, block_id, seeder, size):
        self.id = block_id
        self.seeder = seeder
        self.size = size

    def __repr__(self):
        return '<BlockSpec %s from %s>' % (self.id, self.seeder)



class Block(SharedMemoryData):
    def __init__(self, block_id, allocate=0):
        if not isinstance(block_id, (tuple, list)):
            block_id = (block_id,)
        super().__init__(block_id, allocate)



class BlockNotFound(Exception):
    pass



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

    def __init__(self, node):
        self.node = node
        # blocks by name
        self.blocks = {}  # name : lists
        # protects write access to _available_events
        self._available_lock = asyncio.Lock(loop=self.node.loop)
        # contains events indicating blocks are available (event is set)
        # or are being downloaded (contains name, but event not set)
        self._available_events = {}  # name : threading.Event
        # ranges of bytes available for a block
        self._available_data = {}  # name: [(start, stop), (start, stop), ...]


    def serve_data(self, block_id, data):
        block = Block(block_id, len(data))
        with block.open('wb') as f:
            f.write(data)
        return self.serve_block(block)


    def serve_block(self, block):
        block_size = block.size
        self.blocks[block.id] = block

        if isinstance(block, (InMemoryData, SharedMemoryData)):
            self.node.memory_manager.add_releasable(block.to_disk, block.id, 1, block.size)

        available = threading.Event()
        available.set()
        self._available_events[block.id] = available
        self._available_data[block.id] = [(0, block_size)]

        logger.debug('Serving block %s (%.2f mb)', block.id, block_size / 1024 / 1024)
        return BlockSpec(block.id, self.node.name, block_size)


    def get(self, block_spec, peers=()):
        if block_spec.seeder == self.node.name:
            return self.blocks[block_spec.id]
        else:
            run_coroutine_threadsafe(
                self._download_if_unavailable(None, block_spec, peers),
                self.node.loop
            ).result()
            return self.blocks[block_spec.id]


    def remove_block(self, block_id, from_peers=False):
        '''
        Remove block being served.
        :param block_id: ID of the block to be removed.
        :param from_peers: If True, the block with block_id will be removed from other peer nodes
        as well.
        '''
        self.node.loop.call_soon_threadsafe(self._remove_block, None, block_id)
        if from_peers:
            for peer in self.node.peers.filter():
                peer.service('blocks')._remove_block(block_id)
                # responses aren't waited for


    @asyncio.coroutine
    def _remove_block(self, peer, block_id):
        with (yield from self._available_lock):
            with catch(KeyError):
                del self._available_events[block_id]
            with catch(KeyError):
                del self._available_data[block_id]
            with catch(KeyError):
                self.blocks.pop(block_id).remove()


    @asyncio.coroutine
    def _download_if_unavailable(self, peer, block_spec, peers=()):
        assert block_spec.seeder != self.node.name
        seeder = self.node.peers[block_spec.seeder]

        with (yield from self._available_lock):
            available = self._available_events.get(block_spec.id)
            if available:
                in_progress = True
            else:
                in_progress = False
                available = asyncio.Event(loop=self.node.loop)
                self._available_events[block_spec.id] = available

        if in_progress:
            logger.debug('Download of %r is in progress, waiting for completion', block_spec)
            yield from available.wait()
            if block_spec.id not in self.blocks:
                raise Exception('Unable to download block %r' % block_spec)
        else:
            try:
                download = self._download_from_local if seeder.islocal() else self._download_from_remote
                self.blocks[block_spec.id] = yield from download(block_spec, seeder, peers)
            except Exception as e:
                self._available_events[block_spec.id] = None
                raise Exception('Unable to download block %r: %r' % (block_spec, e)) from e
            finally:
                available.set()



    @asyncio.coroutine
    def _get_chunk(self, peer, block_id, start, end):
        logger.trace('sending chunk %s[%s:%s] to %s', block_id, start, end, peer.name)
        try:
            block = self.blocks[block_id]
        except KeyError:
            logger.info('%r is attempting to download unknown block %r', peer.name, block_id)
            raise BlockNotFound(block_id)
        else:
            assert end <= block.size
            with block.view() as v:
                data = v[start:end]

            assert len(data) == end - start
            return InMemoryData((block_id, start, end), data)


    @asyncio.coroutine
    def _get_available(self, peer, block_id):
        return self._available_data.get(block_id, ())


    @asyncio.coroutine
    def _download_from_remote(self, block_spec, source, peers=()):
        if logger.isEnabledFor(logging.TRACE):
            if block_spec.seeder == source.name:
                logger.trace('Downloading %r from remote seeder', block_spec)
            else:
                logger.trace('Downloading %r from remote peer %r', block_spec, source.name )

        block_id = block_spec.id
        size = block_spec.size

        workers = sorted(
            (
                peer for peer in (peers or self.node.peers.filter())
                if peer.node_type == 'worker' and not peers or peer.name in peers
            ),
            key=lambda worker: '-'.join(map(str, sorted(worker.ip_addresses())))
        )

        local_workers = [worker for worker in workers if worker.islocal()]

        def get_availability(peers):
            futs = [peer.service('blocks')._get_available.request(block_id) for peer in peers]
            return zip(peers, asyncio.as_completed(futs, loop=self.node.loop))

        for peer, available in get_availability(local_workers):
            available = yield from available
            if available == size:
                return (yield from self._download_local(block_spec, peer, peers))

        if self.node.node_type == 'executor':
            assert local_workers

            for worker in local_workers:
                exc = None
                try:
                    yield from worker.service('blocks')._download_if_unavailable.request(block_spec)
                    block = yield from self._download_from_local(block_spec, worker, peers)
                except Exception as e:
                    exc = e
                if exc is not None:
                    raise exc

        else:
            block = Block((self.node.name,) + block_id, size)
            self.blocks[block_id] = block

            chunk_size = size // (len(workers) or 1) // 2
            chunk_size = int(ceil(chunk_size / 1024) * 1024)
            chunk_size = min(chunk_size, bndl.conf['bndl.compute.blocks.min_download_size_mb'] * 1024 * 1024)
            chunk_size = max(chunk_size, bndl.conf['bndl.compute.blocks.max_download_size_mb'] * 1024 * 1024)
            chunk_size = int(chunk_size)

            done = self._available_data[block_id] = []
            todo = [(start, min(size, start + chunk_size))
                    for start in range(0, size, chunk_size)]

            def mark_done(chunk):
                if not done:
                    done.append(chunk)
                    return

                idx = bisect_left(done, chunk)
                match_before = chunk[0] == done[idx - 1][1]
                match_after = idx < len(done) and chunk[1] == done[idx][0]

                if match_before and match_after:
                    done[idx - 1] = done[idx - 1][0], done[idx][1]
                    del done[idx]
                elif match_before:
                    done[idx - 1] = done[idx - 1][0], chunk[1]
                elif match_after:
                    done[idx] = chunk[0], done[idx][1]
                else:
                    done.insert(idx, chunk)

            @asyncio.coroutine
            def download_chunk(node, chunk):
                start, end = chunk
                downloaded = yield from node.service('blocks') \
                                            ._get_chunk.request(block_id, start, end)
                assert downloaded.size == end - start
                size_before = block.size
                with block.open('r+b') as f:
                    f.seek(start)
                    f.write(downloaded.data)
                assert size_before <= block.size, '%s !<= %s' % (size_before, block.size)
                todo.remove(chunk)
                mark_done(chunk)
                logger.trace('Retrieved chunk %s:%s of block %s from %s',
                             start, end, block_id, node.name)

            # keep downloading chunks until all are finished
            # prefer downloading from peers other than the sources
            while todo:
                # find out which peers have data available which isn't available locally
                candidates = []
                for worker, available in get_availability(workers):
                    available = yield from available
                    interesting = []
                    for a_start, a_stop in available:
                        for b_start, b_stop in todo:
                            if a_start <= b_start and a_stop >= b_stop:
                                interesting.append((b_start, b_stop))
                    if interesting:
                        candidates.append((worker, interesting))

                # download all chunks available and locally missing from these candidates
                downloaded = 0
                while candidates:
                    # select a random candidate
                    candidate, interesting = candidates.pop(random.randint(0, len(candidates)) - 1)
                    # select a random chunk
                    chunk = interesting.pop(random.randint(0, len(interesting)) - 1)

                    # download it if still to do (multiple peers may have had this chunk available)
                    if chunk in todo:
                        start, end = chunk
                        try:
                            yield from download_chunk(candidate, chunk)
                            downloaded += 1
                            break
                        except:
                            logger.debug('Unable to retrieve chunk %s:%s of block %s from %s',
                                         start, end, block_id, candidate.name, exc_info=True)

                    # if the peer still has interesting chunks, put it back in the list
                    if interesting:
                        candidates.append((candidate, interesting))

                # download a chunk from the source node
                # if chunks still to do and if none or only one chunk was available at another peer
                if todo and (downloaded <= 1):
                    chunk = todo[random.randint(0, len(todo) - 1)]
                    start, end = chunk
                    yield from download_chunk(source, chunk)

        return block


    @rmi.direct
    def _get_block(self, peer, block_id):
        return self.blocks[block_id]


    @asyncio.coroutine
    def _download_from_local(self, block_spec, source, peers):
        logger.trace('Downloading block %s from local source %s', block_spec.id, source.name)
        block = yield from source.service('blocks')._get_block.request(block_spec.id)
        assert block.size == block_spec.size, '%s != %s' % (block.size, block_spec.size)
        return block
