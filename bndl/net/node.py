from asyncio.futures import CancelledError
import asyncio
import errno
import itertools
import logging
import os
import random
import socket
import weakref

from bndl.net.connection import urlparse, Connection, filter_ip_addresses, \
    getlocalhostname
from bndl.net.peer import PeerNode, PeerTable, HELLO_TIMEOUT
from bndl.net.watchdog import Watchdog
from bndl.util import aio
from bndl.util.aio import get_loop
from bndl.util.exceptions import catch
from bndl.util.supervisor import CHILD_ID
from bndl.util.text import camel_to_snake


logger = logging.getLogger(__name__)


NOTIFY_KNOWN_PEERS_TIMEOUT = 3


class Node(object):
    PeerNode = PeerNode

    _nodeids = itertools.count()

    def __init__(self, name=None, addresses=None, seeds=None, loop=None):
        self.loop = loop or get_loop()
        if name:
            self.name = name
        else:
            self.name = '.'.join(reversed(socket.getfqdn().split('.')))
            child_id = os.environ.get(CHILD_ID)
            if child_id:
                self.name += '.' + str(os.getppid())
                self.name += '.' + child_id
            else:
                self.name += '.' + str(os.getpid())
                self.name += '.' + str(next(Node._nodeids))
        self.node_type = camel_to_snake(self.__class__.__name__)
        self.servers = {address: None for address in (addresses or ())}
        if not self.servers:
            self.servers = {'tcp://%s:%s' % (getlocalhostname(), 5000): None}

        # TODO ensure that if a seed can't be connected to, it is retried
        self.seeds = seeds or ()
        self.peers = PeerTable()
        self._peer_table_lock = asyncio.Lock(loop=self.loop)
        self._watchdog = None
        self._iotasks = weakref.WeakSet()


    @property
    def addresses(self):
        return list(self.servers.keys())


    @property
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


    @asyncio.coroutine
    def _connect_seeds(self):
        for seed in self.seeds:
            if seed not in self.servers:
                yield from self.PeerNode(self.loop, self, addresses=[seed]).connect()


    def stop_async(self):
        return aio.run_coroutine_threadsafe(self.stop(), self.loop)


    def _stop_tasks(self):
        # stop watching
        with catch():
            self._watchdog.stop()
            self._watchdog = None

        # close the servers
        for server in self.servers.values():
            with catch():
                server.close()

        # cancel any pending io work
        for task in self._iotasks:
            with catch():
                task.cancel()
        self._iotasks.clear()


    @asyncio.coroutine
    def stop(self):
        # close the watch dog, tasks and the servers
        self._stop_tasks()

        # disconnect from the peers
        if self.peers:
            disconnects = [peer.disconnect('stopping node') for peer in list(self.peers.values())]
            yield from asyncio.wait(disconnects, loop=self.loop)
            self.peers.clear()


    @property
    def running(self):
        return any(server and server.sockets for server in self.servers.values())


    @asyncio.coroutine
    def _start_server(self, address):
        parsed = urlparse(address)
        if parsed.scheme != 'tcp':
            raise ValueError('unsupported scheme %s in address %s' % (parsed.scheme, address))

        host, port = parsed.hostname, parsed.port or 5000
        server = None

        for port in range(port, port + 1000):
            try:
                server = yield from asyncio.start_server(self._serve, host, port, loop=self.loop)
                break
            except OSError as exc:
                if exc.errno == errno.EADDRINUSE:
                    continue
                else:
                    logger.exception('unable to open server socket')

        if not server:
            return

        if parsed.port != port:
            del self.servers[address]
            address = 'tcp://%s:%s' % (host, port)
        logger.info('server socket opened at %s', address)
        self.servers[address] = server


    @asyncio.coroutine
    def _discovered(self, src, discovery):
        for name, addresses in discovery.peers:
            logger.debug('%s: %s discovered %s', self.name, src.name, name)
            with(yield from self._peer_table_lock):
                if name not in self.peers:
                    try:
                        peer = self.PeerNode(self.loop, self, addresses=addresses, name=name)
                        yield from peer.connect()
                    except Exception:
                        logger.warning('unexpected error while connecting to discovered peer %s', name)


    @asyncio.coroutine
    def _serve(self, reader, writer):
        try:
            conn = Connection(self.loop, reader, writer)
            yield from self.PeerNode(self.loop, self)._connected(conn)
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
                return
            known_peer = self.peers.get(peer.name)
            if known_peer and known_peer.is_connected and peer is not known_peer:
                # perform a 'tie brake' between the two connections with the peer
                # this is to prevent situations where two nodes 'call each other'
                # at the same time
                if known_peer < peer:
                    # existing connection wins
                    logger.debug('already connected with %s, closing %s', peer.name, known_peer.conn)
                    yield from peer.disconnect(reason='already connected, old connection wins')
                    return
                else:
                    # new connection wins
                    logger.debug('already connected with %s, closing %s', peer.name, known_peer.conn)
                    yield from known_peer.disconnect(reason='already connected, new connection wins')
            self.peers[peer.name] = peer

        # notify others of the new peer
        task = self.loop.create_task(self._notifiy_peers(peer))
        self._iotasks.add(task)

        return True


    @asyncio.coroutine
    def _notifiy_peers(self, new_peer):
        with(yield from self._peer_table_lock):
            peers = list(self.peers.filter())
            random.shuffle(peers)

        peer_list = list(
            (peer.name, peer.addresses)
            for peer in peers
            if peer.name != new_peer.name
        )

        try:
            if peer_list:
                yield from new_peer._notify_discovery(peer_list)
        except CancelledError:
            return
        except Exception:
            logger.exception('discovery notification failed')

        yield from asyncio.sleep(NOTIFY_KNOWN_PEERS_TIMEOUT, loop=self.loop)

        for peer in peers:
            if peer.name != new_peer.name:
                try:
                    yield from peer._notify_discovery([(new_peer.name, new_peer.addresses)])
                except CancelledError:
                    return
                except Exception:
                    logger.debug('discovery notification failed', exc_info=True)


    def __str__(self):
        return 'Node ' + self.name
