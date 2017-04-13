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

from asyncio.futures import CancelledError
import asyncio
import concurrent.futures
import errno
import itertools
import logging
import os
import random
import socket

from bndl.net import aio
from bndl.net.aio import get_loop, IOTasks
from bndl.net.connection import urlparse, Connection, filter_ip_addresses
from bndl.net.peer import PeerNode, PeerTable
from bndl.net.watchdog import Watchdog
from bndl.util.exceptions import catch
from bndl.util.strings import camel_to_snake
import bndl


logger = logging.getLogger(__name__)


NOTIFY_KNOWN_PEERS_WAIT = 1


class Node(IOTasks):
    PeerNode = PeerNode

    _nodeids = {}

    def __init__(self, name=None, node_type=None, addresses=None, seeds=None, cluster='default', loop=None):
        super().__init__()

        self.loop = loop or get_loop()
        self.node_type = node_type or camel_to_snake(self.__class__.__name__)

        if name:
            self.name = name
        else:
            self.name = '.'.join(reversed(socket.getfqdn().split('.'))) + \
                        '.' + self.node_type + \
                        '.' + str(os.getpid())

            node_ids = Node._nodeids.setdefault(self.node_type, itertools.count())
            node_id = next(node_ids)
            self.name += '.' + str(node_id + 1)

        addresses = addresses or bndl.conf['bndl.net.listen_addresses']
        seeds = seeds or bndl.conf['bndl.net.seeds']

        self.servers = {address: None
                        for address in
                        (addresses or ())}

        self.seeds = seeds or ()
        self.peers = PeerTable()

        self.cluster = cluster

        self._peer_table_lock = asyncio.Lock(loop=self.loop)
        self._watchdog = None


    @property
    def addresses(self):
        return list(self.servers.keys())


    def ip_addresses(self):
        return filter_ip_addresses(*self.addresses)


    def start_async(self):
        return aio.run_coroutine_threadsafe(self.start(), self.loop)


    @asyncio.coroutine
    def start(self):
        if self.running:
            return
        for address in list(self.servers.keys()):
            yield from self._start_server(address)
        # connect with seeds
        yield from self._connect_seeds()
        # start the watchdog
        self._watchdog = Watchdog(self)
        self._watchdog.start()

        logger.debug('Node %r started', self.name)


    @asyncio.coroutine
    def _connect_seeds(self):
        for seed in self.seeds:
            if seed not in self.servers:
                peers = self.peers.filter(address=seed, connected=None)
                if peers:
                    for peer in peers:
                        yield from peer.connect()
                else:
                    yield from self.PeerNode(self.loop, self, addresses=[seed]).connect()


    def stop_async(self):
        if not self.loop.is_closed():
            return aio.run_coroutine_threadsafe(self.stop(), self.loop)
        else:
            self._stop_tasks()
            fut = concurrent.futures.Future()
            fut.set_result(None)
            return fut


    def _stop_tasks(self):
        # stop watching
        if self._watchdog:
            with catch(RuntimeError, log_level=logging.WARNING):
                self._watchdog.stop()
                self._watchdog = None

        super()._stop_tasks()

        # close the servers
        for server in self.servers.values():
            if server:
                with catch(RuntimeError, log_level=logging.WARNING):
                    server.close()


    @asyncio.coroutine
    def stop(self):
        # disconnect from the peers
        if self.peers:
            disconnects = [peer.disconnect('stopping node') for peer in list(self.peers.values())]
            yield from asyncio.wait(disconnects, loop=self.loop)
            self.peers.clear()

        # close the watch dog, tasks and the servers
        self._stop_tasks()


    @property
    def running(self):
        try:
            return any(server and server.sockets for server in self.servers.values())
        except Exception:
            return False


    @asyncio.coroutine
    def _start_server(self, address):
        parsed = urlparse(address)
        if parsed.scheme != 'tcp':
            raise ValueError('unsupported scheme %s in address %s' % (parsed.scheme, address))

        host, port = parsed.hostname, 5000 if parsed.port is None else parsed.port
        server = None

        for port in range(port, port + 1000):
            try:
                server = yield from asyncio.start_server(self.serve, host, port, loop=self.loop)
                break
            except OSError as exc:
                if exc.errno == errno.EADDRINUSE:
                    continue
                else:
                    logger.exception('unable to open server socket')

        if not server:
            return

        del self.servers[address]

        for s in server.sockets:
            port = s.getsockname()[1]
            address = 'tcp://%s:%s' % (host, port)
            logger.info('server socket opened at %s', address)
            self.servers[address] = server


    @asyncio.coroutine
    def _discovered(self, src, discovery):
        for name, addresses in discovery.peers:
            with(yield from self._peer_table_lock):
                if name not in self.peers:
                    try:
                        logger.debug('%s: %s discovered %s', self.name, src.name, name)
                        peer = self.PeerNode(self.loop, self, addresses=addresses, name=name)
                        yield from peer.connect()
                    except Exception:
                        logger.warning('unexpected error while connecting to discovered peer %s', name, exc_info=True)


    @asyncio.coroutine
    def serve(self, reader, writer):
        try:
            conn = Connection(self.loop, reader, writer)
            peer = self.PeerNode(self.loop, self)
            yield from peer.connected(conn)
        except GeneratorExit:
            conn.close()
        except Exception:
            with catch():
                conn.close()
                logger.exception('unable to accept connection from %s', conn.peername())


    @asyncio.coroutine
    def _peer_connected(self, peer):
        with (yield from self._peer_table_lock):
            if self.name == peer.name:
                # don't allow connection loops
                logger.debug('self connect attempt of %s', peer.name)
                yield from peer.disconnect(reason='self connect')
                return False

            known_peer = self.peers.get(peer.name)
            if not known_peer:
                for address in peer.addresses:
                    matches = self.peers.filter(address=address, connected=None)
                    if matches:
                        known_peer = matches[0]
                        if known_peer.is_connected:
                            logger.warning('Peers %s and %s (of %s) share addresses (%s) and are both connected',
                                           known_peer.name, peer.name, self.name,
                                           list(known_peer.addresses) + list(peer.addresses))
                        break

            if known_peer:
                if known_peer.is_connected and peer is not known_peer:
                    # perform a 'tie brake' between the two connections with the peer
                    # this is to prevent situations where two nodes 'call each other'
                    # at the same time
                    if known_peer < peer:
                        # existing connection wins
                        logger.debug('already connected with %s, closing new connection %s', peer.name, known_peer.conn)
                        yield from peer.disconnect(reason='already connected, old connection wins')
                        return False

                if known_peer.name != peer.name:
                    with catch(KeyError):
                        del self.peers[known_peer.name]

            self.peers[peer.name] = peer

        # notify others of the new peer
        if peer.cluster:
            self._create_task(self._notifiy_peers(peer))

        return True


    @asyncio.coroutine
    def _notifiy_peers(self, new_peer):
        with(yield from self._peer_table_lock):
            peers = self.peers.filter()

        random.shuffle(peers)
        peer_list = [
            (peer.name, peer.addresses)
            for peer in peers
            if peer.name != new_peer.name
        ]

        try:
            if peer_list:
                yield from new_peer.notify_discovery(peer_list)
        except CancelledError:
            return
        except Exception:
            logger.exception('discovery notification failed')

        for peer in peers:
            if peer.name != new_peer.name:
                try:
                    yield from asyncio.sleep(NOTIFY_KNOWN_PEERS_WAIT, loop=self.loop)
                    yield from peer.notify_discovery([(new_peer.name, new_peer.addresses)])
                except CancelledError:
                    return
                except Exception:
                    logger.debug('discovery notification failed', exc_info=True)


    def __str__(self):
        return 'Node ' + self.name
