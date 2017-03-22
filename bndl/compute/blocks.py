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

import asyncio
import logging
import mmap
import os
import random
import socket
import struct
import tempfile
import threading
import weakref

from bndl import rmi
from bndl.compute.storage import Data, get_work_dir
from bndl.util.aio import run_coroutine_threadsafe
from bndl.util.exceptions import catch
from math import ceil
from bisect import bisect_left
from bndl.util import serialize


logger = logging.getLogger(__name__)


AVAILABILITY_TIMEOUT = 1

MIN_DOWNLOAD_SIZE = 1024 * 1024
MAX_DOWNLOAD_SIZE = 1024 * 1024 * 8


class BlockSpec(object):
    def __init__(self, block_id, seeder, size):
        self.id = block_id
        self.seeder = seeder
        self.size = size

    def __repr__(self):
        return '<BlockSpec %s from %s>' % (self.id, self.seeder)



class Block(object):
    def __init__(self, block_id, size, fd=-1):
        self.id = block_id
        self.size = size

        if fd == -1:
            self.fd, name = tempfile.mkstemp(prefix='block-', dir=get_work_dir())
            os.unlink(name)
            with open(self.fd, 'wb', closefd=False) as f:
                f.write(b'0')
            self.data = mmap.mmap(self.fd, 1)
            self.data.resize(self.size)

        else:
            self.fd = fd
            self.data = mmap.mmap(self.fd, size)

        weakref.finalize(self, os.close, self.fd)



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

        self.fd_server = None
        self.fd_socket_path = None

    @asyncio.coroutine
    def start(self):
        dirpath = tempfile.mkdtemp(prefix='blocks-', dir=get_work_dir())
        self.fd_socket_path = os.path.join(dirpath, 'blocks.socket')
        self.fd_server = yield from asyncio.start_unix_server(self._serve_block_fd, self.fd_socket_path, loop=self.node.loop)


    @asyncio.coroutine
    def stop(self):
        self.fd_server.close()
        if os.path.exists(self.fd_socket_path):
            os.unlink(self.fd_socket_path)


    def serve_data(self, block_id, data):
        block = Block(block_id, len(data))
        block.data[:] = data
        return self.serve_block(block)


    def serve_block(self, block):
        self.blocks[block.id] = block
        available = threading.Event()
        available.set()
        self._available_events[block.id] = available
        self._available_data[block.id] = [(0, block.size)]
        logger.debug('Serving block %s (%.2f mb)', block.id, block.size / 1024 / 1024)
        return BlockSpec(block.id, self.node.name, block.size)


    def get(self, block_spec, peers=()):
        if block_spec.seeder == self.node.name:
            return self.blocks[block_spec.id].data
        else:
            run_coroutine_threadsafe(
                self._download_if_unavailable(None, block_spec, peers),
                self.node.loop
            ).result()
            return self.blocks[block_spec.id].data


    def remove_block(self, block_id, from_peers=False):
        '''
        Remove block being served.
        :param block_id: ID of the block to be removed.
        :param from_peers: If True, the block with block_id will be removed from other peer nodes
        as well.
        '''
        self._remove_block(None, block_id)
        if from_peers:
            for peer in self.node.peers.filter():
                peer._remove_block(block_id)
                # responses aren't waited for


    @rmi.direct
    def _remove_block(self, peer, block_id):
        with catch(KeyError):
            del self.blocks[block_id]
        with catch(KeyError):
            del self._available_events[block_id]
        with catch(KeyError):
            del self._available_data[block_id]


    @asyncio.coroutine
    def _serve_block_fd(self, r, w):
        header = yield from r.readexactly(5)
        block_id_len = struct.unpack('I', header[:4])[0]
        block_id_msg = yield from r.readexactly(block_id_len)
        marshalled = header[4:] == b'M'
        block_id = serialize.loads(marshalled, block_id_msg)

#         print('serving fd for block_id', block_id)

        try:
            fd = self.blocks[block_id].fd
        except KeyError:
            w.close()
        else:
            sock = w.transport.get_extra_info('socket')

            def send_fd():
                msg = struct.pack('I', fd)
                sock.setblocking(True)
                try:
#                     print('sending fd')
                    sock.sendmsg([bytes([len(msg)])], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, msg)])
                except:
                    import traceback;traceback.print_exc()
                    raise
                finally:
                    sock.setblocking(False)
#                 print('fd sent')

            yield from self.node.loop.run_in_executor(None, send_fd)

            # wait for other end to close the connection
            try:
                yield from r.read()
            except ConnectionResetError:
                pass


    @rmi.direct
    def _get_socket_path(self, peer):
        return self.fd_socket_path


    def _receive_fd(self, path, block_id):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)

        marshalled, msg = serialize.dumps(block_id)
        msg_len = struct.pack('I', len(msg))
        msg = msg_len + (b'M' if marshalled else b'P') + msg
        sock.sendall(msg)

        msg = sock.recvmsg(0, socket.CMSG_LEN(4))
        if not msg:
            return None
        else:
            ancdata = msg[1]
            cmsg_level, cmsg_type, cmsg_data = ancdata[0]
            assert cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS
            fd = struct.unpack('I', cmsg_data)[0]
            return fd


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
            yield from available.wait()
            if block_spec.id not in self.blocks:
                raise Exception('Unable to download block %r' % block_spec)
        else:
            try:
                download = self._download_from_local if seeder.islocal() else self._download_from_remote
                self.blocks[block_spec.id] = yield from download(block_spec, seeder, peers)
            except Exception as e:
                self._available_events[block_spec.id] = None
                raise Exception('Unable to download block %r' % block_spec) from e
            finally:
                available.set()



    @asyncio.coroutine
    def _get_chunk(self, peer, block_id, start, end):
        logger.trace('sending chunk %s[%s:%s] to %s', block_id, start, end, peer.name)
        return Data((block_id, start, end), self.blocks[block_id].data[start:end])


    @asyncio.coroutine
    def _get_available(self, peer, block_id):
        return self._available_data.get(block_id, ())


    @asyncio.coroutine
    def _download_from_remote(self, block_spec, source, peers=()):
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
            block = Block(block_id, size)
            self.blocks[block_id] = block
            data = block.data

            chunk_size = size // (len(workers) or 1) // 2
            chunk_size = int(ceil(chunk_size / 1024) * 1024)
            chunk_size = min(chunk_size, MAX_DOWNLOAD_SIZE)
            chunk_size = max(chunk_size, MIN_DOWNLOAD_SIZE)

            done = self._available_data[block_id] = []
            todo = [(start, start + chunk_size)
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

                data[start:end] = downloaded.data
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
                            print(self.node.name, 'downloaded', start, ':', end,
                                  'from peer', candidate.name, ':)',
                                  len(todo), 'chunks remaining')
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
                    print(self.node.name, 'downloaded', start, ':', end,
                          'from source', source.name, ':(',
                          len(todo), 'chunks remaining')

        return block


#     @asyncio.coroutine
#     def _download_from_local(self, block_spec, source, peers):
#         logger.trace('Downloading %s from %s', block_spec, source.name)
#
#         # ask source for the path of the unix socket for fd access
#         socket_path = yield from source.service('blocks')._get_socket_path.request()
#
#         # connect
#         r, w = yield from asyncio.open_unix_connection(socket_path, loop=self.node.loop)
#         transport = w.transport
#         conn = Connection(self.node.loop, r, w)
#
#         # send the block id for which the fd should be accessed
#         yield from conn.send(block_spec.id)
#         available = yield from conn.recv()
#         if not available:
#             return
#
#         def receive_fd():
#             sock = transport.get_extra_info('socket')
#             sock.setblocking(True)
#             try:
#                 msg = sock.recvmsg(1, socket.CMSG_LEN(4))
#             finally:
#                 sock.setblocking(False)
#             ancdata = msg[1]
#             cmsg_level, cmsg_type, cmsg_data = ancdata[0]
#             assert cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS
#             fd = struct.unpack('I', cmsg_data)[0]
#             return fd
#
#         # receive the fd (with sock.recvmsg in blocking mode)
#         transport.pause_reading()
#         fd = yield from self.node.loop.run_in_executor(None, receive_fd)
#         transport.resume_reading()
#         yield from conn.close()
#
#         return Block(block_spec.id, block_spec.size, fd)


    @asyncio.coroutine
    def _download_from_local(self, block_spec, source, peers):
        logger.trace('Downloading %s from %s', block_spec, source.name)

        # ask source for the path of the unix socket for fd access
        path = yield from source.service('blocks')._get_socket_path.request()
        # read the file descriptor
        fd = yield from self.node.loop.run_in_executor(None, self._receive_fd, path, block_spec.id)
        # mmap the 'file'
        return Block(block_spec.id, block_spec.size, fd)
