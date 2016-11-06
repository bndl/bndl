from asyncio.futures import CancelledError
from datetime import datetime
import asyncio
import atexit
import errno
import logging

from bndl.net.connection import urlparse, Connection, NotConnected, \
    filter_ip_addresses
from bndl.net.messages import Hello, Discovered, Disconnect, Ping, Pong, Message
from bndl.util import aio
from bndl.util.exceptions import catch
import concurrent.futures


logger = logging.getLogger(__name__)


HELLO_TIMEOUT = 60


class PeerTable(dict):
    def filter(self, address=None, node_type=None, connected=True):
        while True:
            try:
                peers = self.values()
                if address is not None:
                    peers = filter(lambda p: address in p.addresses, peers)
                if node_type is not None:
                    peers = filter(lambda p: p.node_type == node_type, peers)
                if connected is not None:
                    peers = filter(lambda p: p.is_connected == connected, peers)
                return list(peers)
            except RuntimeError:
                pass


class PeerNode(object):
    def __init__(self, loop, local, addresses=(), name=None, node_type=None):
        self.loop = loop
        self.local = local
        self.addresses = addresses
        self.name = name
        self.node_type = node_type
        self.handshake_lock = asyncio.Lock(loop=self.loop)
        self.conn = None
        self.server = None
        self.connected_on = None
        self.disconnected_on = None
        self._iotasks = set()

        atexit.register(self._stop_tasks)


    def ip_addresses(self):
        return filter_ip_addresses(*self.addresses)


    def islocal(self):
        return bool(self.ip_addresses() & self.local.ip_addresses())


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
        yield from self.conn.send(msg.__msgdict__(), drain)


    @asyncio.coroutine
    def recv(self, timeout=None):
        msg = yield from self.conn.recv(timeout)
        return Message.load(msg)


    @property
    def is_connected(self):
        return bool(self.conn and self.conn.is_connected)


    @asyncio.coroutine
    def connect(self):
        if self.is_connected:
            return

        connected = False
        for address in self.addresses:
            connected = (yield from self._connect(address))
            if connected:
                break


    def disconnect_async(self, reason='', active=True):
        if not self.loop.is_closed():
            return aio.run_coroutine_threadsafe(self.disconnect(reason, active), self.loop)
        else:
            self._stop_tasks()
            fut = concurrent.futures.Future()
            fut.set_result(None)
            return fut


    def _stop_tasks(self):
        # cancel any pending io work
        for task in self._iotasks:
            with catch(RuntimeError):
                task.cancel()
        self._iotasks.clear()

        # close the servers
        if self.server:
            with catch(RuntimeError):
                self.server.cancel()


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

        # close the io tasks and the server
        self._stop_tasks()

        # close the connection
        if self.conn:
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

                reader, writer = yield from asyncio.open_connection(address.hostname, address.port, loop=self.loop)
                self.conn = Connection(self.loop, reader, writer)
                logger.debug('%s connected', self.conn)

                # Say hello
                yield from self._send_hello()

                # wait for hello back
                logger.debug('waiting for hello from %s', url)
                hello = yield from self.recv(HELLO_TIMEOUT)

                # check the hello
                if isinstance(hello, Disconnect):
                    logger.debug("received Disconnect from %s, disconnecting", url)
                    yield from self.disconnect(reason="received disconnect", active=False)
                elif not isinstance(hello, Hello):
                    logger.error("didn't receive Hello back from %s, disconnecting", url)
                    yield from self.disconnect(reason="didn't receive hello")
                elif hello.name == self.local.name:
                    logger.debug('self connect attempt of %s', hello.name)
                    yield from self.disconnect(reason='self connect')
                elif self.name is not None and self.name != hello.name:
                    logger.debug('node %s at %s changed name to %s', self.name, self.addresses, hello.name)
                    try:
                        del self.local.peers[self.name]
                    except KeyError:
                        pass
                    self.local.peers[hello.name] = self

            except (asyncio.futures.CancelledError, GeneratorExit):
                logger.info('connection with %s cancelled', url)
                yield from self.disconnect(reason='connection cancelled')
            except (FileNotFoundError, ConnectionResetError, ConnectionRefusedError, NotConnected) as exc:
                logger.info('%s %s', type(exc).__name__, url)
                yield from self.disconnect(reason='unable to connect: ' + str(type(exc)), active=False)
            except OSError as exc:
                logger.info('unable to connect with %s on %s', url, self.conn,
                            exc_info=bool(exc.errno in (errno.ECONNREFUSED, errno.ECONNRESET)))
                yield from self.disconnect(reason='unable to connect: ' + str(type(exc)), active=False)
            except TimeoutError:
                logger.warning('hello not received in time from %s on %s', url, self.conn)
                yield from self.disconnect(reason='hello timed out')
            except Exception as exc:
                logger.exception('unable to connect with %s on %s', url, self.conn)
                yield from self.disconnect(reason=str(type(exc)))

            if not self.is_connected:
                return False

            # take info from hello
            self.name = hello.name
            self.node_type = hello.node_type
            self.addresses = hello.addresses

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
                hello = yield from self.recv(HELLO_TIMEOUT)
            except TimeoutError:
                logger.warning('receiving hello timed out from %s', self.conn.peername())
                yield from self.disconnect(reason='hello timed out')
            except NotConnected:
                logger.warning('connection closed before receiving hello from %s', self.conn.peername())
                yield from self.disconnect(reason='connection closed')
            except Exception as exc:
                logger.exception('unable to read hello from %s', self.conn.peername())
                yield from self.disconnect(reason=str(type(exc)))

            if not self.is_connected:
                return

            if hello.name == self.local.name:
                logger.debug('self connect attempt of %s', hello.name)
                yield from self.disconnect(reason='self connect')

            logger.debug('hello received from %s at %s', hello.name, hello.addresses)

            try:
                yield from self._send_hello()
            except NotConnected:
                return
            except Exception:
                logger.exception('unable to complete handshake with %s', hello.name)
                connection.close()

            self.name = hello.name
            self.node_type = hello.node_type
            self.addresses = hello.addresses

            logger.debug('handshake between %s and %s complete', self.local.name, self.name)

            self.server = self.loop.create_task(self._serve())


    @asyncio.coroutine
    def _send_hello(self):
        if not self.is_connected:
            raise NotConnected()
        yield from self.send(Hello(
            name=self.local.name,
            node_type=self.local.node_type,
            addresses=list(self.local.servers.keys()),
        ), drain=True)


    @asyncio.coroutine
    def _serve(self):
        self.connected_on = datetime.now()
        self.disconnected_on = None

        if not self.is_connected:
            return

        yield from self.local._peer_connected(self)

        if self.is_connected:
            logger.info('serving connection for %s (local) with %s (remote) on %s',
                        self.local.name, self.name, self.conn)

        while self.is_connected:
            try:
                # logger.debug('%s is waiting for message from %s', self.local.name, self.name)
                msg = yield from self.recv()
                # logger.debug('%s received message from %s: %s', self.local.name, self.name, msg)
            except NotConnected:
                logger.debug('read EOF on connection with %s', self.name)
                yield from self.disconnect('received EOF', active=False)
                break
            except asyncio.futures.CancelledError:
                logger.debug('connection with %s cancelled', self.name)
                yield from self.disconnect('connection cancelled', active=False)
                break
            except ConnectionResetError:
                logger.debug('connection with %s closed unexpectedly', self.name)
                yield from self.disconnect('connection reset', active=False)
                break
            except asyncio.streams.IncompleteReadError:
                logger.exception('connection with %s closed unexpectedly', self.name)
                yield from self.disconnect('connection reset', active=False)
                break
            except Exception as exc:
                logger.exception('An unknown exception occurred in connection %s', self.name)
                yield from self.disconnect('unexpected error: ' + str(exc), active=False)
                break

            task = self.loop.create_task(self._dispatch(msg))
            self._iotasks.add(task)
            task.add_done_callback(self._iotasks.discard)

        logger.debug('connection between %s (local) and %s (remote) closed', self.local.name, self.name)
        self.disconnected_on = datetime.now()


    @asyncio.coroutine
    def _notify_discovery(self, peers):
        if not peers:
            return
        try:
            logger.debug('notifying %s of discovery of %s', self.name, peers)
            yield from self.send(Discovered(peers=peers))
        except NotConnected:
            pass


    @asyncio.coroutine
    def _dispatch(self, msg):
        try:
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
        except CancelledError:
            pass
        except Exception:
            logger.exception('unable to dispatch message %s', type(msg))


    def __lt__(self, other):
        if not isinstance(other, PeerNode):
            raise ValueError('comparision of PeerNodes is only defined for other PeerNodes')
        return self.conn < other.conn


    def __repr__(self):
        return '<Peer: %s>' % self.name
