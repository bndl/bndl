import asyncio
import logging
import os.path
import socket

from bndl.net.connection import urlparse, Connection, NotConnected, \
    filter_ip_addresses
from bndl.net.messages import Hello, Discovered, Disconnect, Ping, Pong
from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)



HELLO_TIMEOUT = 60


class PeerTable(dict):
    def filter(self, address=None, node_type=None, connected=True):
        peers = self.values()
        if address is not None:
            peers = filter(lambda p: address in p.addresses, peers)
        if node_type is not None:
            peers = filter(lambda p: p.node_type == node_type, peers)
        if connected is not None:
            peers = filter(lambda p: p.is_connected == connected, peers)
        return list(peers)


class PeerNode(object):
    def __init__(self, loop, local, addresses=(), name=None, node_type=None):
        self.loop = loop
        self.local = local
        self.addresses = addresses
        self.name = name
        self.node_type = node_type
        self.handshake_lock = asyncio.Lock()
        self.conn = None
        self.server = None


    @property
    def ip_addresses(self):
        return set(filter_ip_addresses(self.addresses))

    @property
    def islocal(self):
        return bool(self.ip_addresses & self.local.ip_addresses)


    @asyncio.coroutine
    def send(self, msg, drain=False):
        '''
        Send a message to the peer.
        :param msg: Message
        :param drain: boolean
            Whether to complete only after the message has been written out
            to the network.
        '''
        if not self.is_connected:
            raise NotConnected()
        logger.debug('sending %s to %s', msg.__class__.__name__, self.name)
        yield from self.conn.send(msg, drain)


    @property
    def is_connected(self):
        return bool(self.conn and self.conn.is_connected)


    @asyncio.coroutine
    def connect(self):
        if self.is_connected:
            return

        connected = False
        for address in sorted(self.addresses, key=lambda a: urlparse(a).scheme, reverse=True):
            connected = (yield from self._connect(address))
            if connected:
                break


    @asyncio.coroutine
    def disconnect(self, reason='', active=True):
        logger.log(logging.INFO if active and self.is_connected else logging.DEBUG,
                   '%s (local) disconnected from %s (remote) with reason: %s (%s disconnect)',
                   self.local.name, self.name,
                   reason, 'active' if active and self.is_connected else 'passive')
        # possibly notify the other end
        if active and self.is_connected:
            with catch():
                yield from self.send(Disconnect(reason=reason), drain=True)
        # close the server task
        with catch():
            self.server.cancel()
        # close the connection
        with catch():
            yield from self.conn.close()
        # clear the fields
        self.server = None
        self.conn = None


    @asyncio.coroutine
    def _connect(self, url):
        with (yield from self.handshake_lock):
            if self.is_connected:
                return

            logger.debug('connecting with %s', url)

            try:
                address = urlparse(url)
                if address.scheme != 'tcp':
                    raise ValueError('unsupported scheme in %s', url)

                rw = yield from asyncio.open_connection(address.hostname, address.port)
                self.conn = Connection(self.loop, *rw)
                logger.debug('%s connected', self.conn)

                # Say hello
                yield from self._send_hello()

                # wait for hello back
                logger.debug('waiting for hello from %s', url)
                rep = yield from self.conn.recv(HELLO_TIMEOUT)

                if isinstance(rep, Disconnect):
                    logger.debug("received Disconnect from %s, disconnecting", url)
                    yield from self.disconnect(reason="received disconnect", active=False)
                elif not isinstance(rep, Hello):
                    logger.error("didn't receive Hello back from %s, disconnecting", url)
                    yield from self.disconnect(reason="didn't receive hello")
                elif self.name and self.name != rep.name:
                    # check if reported name and expected name (if any) agree
                    logger.error('shaking hands with peer at %s but claims to have the name %s instead of %s, '
                                 'disconnecting ...', self.addresses, rep.name, self.name)
                    yield from self.disconnect(reason='name mismatch')
                elif rep.name == self.local.name:
                    logger.debug('self connect attempt of %s', rep.name)
                    yield from self.disconnect(reason='self connect')
            except asyncio.futures.CancelledError:
                logger.info('connection with %s cancelled', url)
                self.disconnect(reason='connection cancelled')
            except (FileNotFoundError, ConnectionResetError, ConnectionRefusedError) as e:
                logger.info('%s %s', type(e).__name__, url)
                self.disconnect(reason='unable to connect: ' + str(type(e)))
            except TimeoutError:
                logger.warning('hello not received in time from %s on %s', url, self.conn)
                yield from self.disconnect(reason='hello timed out')
            except Exception as e:
                logger.exception('unable to connect with %s on %s', url, self.conn)
                yield from self.disconnect(reason=str(type(e)))

            if not self.is_connected:
                return False

            # take info from hello
            self.name = rep.name
            self.node_type = rep.node_type
            self.addresses = rep.addresses

            # notify local node that connection was established and start a server task
            logger.debug('handshake between %s and %s complete', self.local.name, self.name)
            self.server = self.loop.create_task(self._serve())
            return True


    @asyncio.coroutine
    def _connected(self, connection):
        logger.debug('%s connected', connection)

        self.conn = connection

        with (yield from self.handshake_lock):
            try:
                hello = yield from connection.recv(HELLO_TIMEOUT)
            except TimeoutError:
                logger.warning('receiving hello timed out from %s', self.conn.peername())
                self.disconnect(reason='hello timed out')
            except NotConnected:
                logger.warning('connection closed before receiving hello from %s', self.conn.peername())
                self.disconnect(reason='connection closed')
            except Exception as e:
                logger.exception('unable to read hello from %s', self.conn.peername())
                self.disconnect(reason=str(type(e)))

            if hello.name == self.local.name:
                logger.debug('self connect attempt of %s', hello.name)
                yield from self.disconnect(reason='self connect')

            if not self.is_connected:
                return

            logger.debug('hello received from %s at %s', hello.name, hello.addresses)

            try:
                yield from self._send_hello()
            except:
                logger.exception('unable to complete handshake with %s', hello.name)
                connection.close()

            self.name = hello.name
            self.node_type = hello.node_type
            self.addresses = hello.addresses

            logger.debug('handshake between %s and %s complete', self.local.name, self.name)
            self.server = self.loop.create_task(self._serve())


    @asyncio.coroutine
    def _send_hello(self):
        yield from self.conn.send(Hello(
            name=self.local.name,
            node_type=self.local.node_type,
            addresses=list(self.local.servers.keys()),
        ), drain=True)


    @asyncio.coroutine
    def _serve(self):
        if not self.is_connected:
            return

        yield from self.local._peer_connected(self)

        if self.is_connected:
            logger.info('serving connection for %s (local) with %s (remote) on %s', self.local.name, self.name, self.conn)

        while self.is_connected:
            try:
                # logger.debug('%s is waiting for message from %s', self.local.name, self.name)
                msg = yield from self.conn.recv()
                # logger.debug('%s received message from %s: %s', self.local.name, self.name, msg)
            except NotConnected:
                logger.debug('read EOF on connection with %s', self.name)
                yield from self.disconnect('received EOF')
                break
            except asyncio.futures.CancelledError:
                logger.debug('connection with %s cancelled', self.name)
                yield from self.disconnect('connection cancelled')
                break
            except ConnectionResetError:
                logger.warning('connection with %s closed unexpectedly', self.name)
                yield from self.disconnect('connection reset')
                break
            except asyncio.streams.IncompleteReadError:
                logger.exception('connection with %s closed unexpectedly', self.name)
                yield from self.disconnect('connection reset')
                break
            except Exception as e:
                logger.exception('An unknown exception occurred in connection %s', self.name)
                yield from self.disconnect('unexpected error: ' + str(e))
                break

            self.loop.create_task(self._dispatch(msg))

        logger.debug('connection between %s (local) and %s (remote) closed', self.local.name, self.name)


    @asyncio.coroutine
    def _notify_discovery(self, peers):
        if not peers:
            return
        try:
            logger.debug('notifying %s of discovery of %s', self.name, peers)
            yield from self.send(Discovered(peers=peers))
        except NotConnected:
            logger.debug('not connected')
            pass


    @asyncio.coroutine
    def _dispatch(self, msg):
        logger.debug('dispatching %s', msg)
        if isinstance(msg, Disconnect):
            yield from self.disconnect(reason='received disconnect', active=False)
        elif isinstance(msg, Discovered):
            yield from self.local._discovered(self, msg)
        elif isinstance(msg, Ping):
            yield from self.send(Pong())
        elif isinstance(msg, Pong):
            pass
        else:
            logger.warning('message of unsupported type %s %s', type(msg), self)



    def __lt__(self, other):
        if not isinstance(other, PeerNode):
            raise ValueError('comparision of PeerNodes is only defined for other PeerNodes')

        return self.conn < other.conn


    def __str__(self):
        # return 'Peer %s of %s (%sconnected)' % (self.name, self.local.name, '' if self.is_connected() else 'not ')
        # return '%s - Peer %s of %s' % (id(self), self.name, self.local.name)
        return 'Node ' + self.local.name + ' - Peer ' + (self.name or '?')
