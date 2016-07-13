import asyncio
import functools
import logging
import socket
import struct
import sys
import urllib.parse

from bndl.net import serialize
from bndl.util import aio
from bndl.util.aio import async_call


logger = logging.getLogger(__name__)



def urlparse(address):
    '''
    Parse an address urllib.parse.urlparse and checking validity in the context
    of bndl.
    :param address: str
    '''
    parsed = urllib.parse.urlparse(address)

    if parsed.scheme == 'tcp':
        if parsed.path:
            raise ValueError("Path not supported in tcp address: " + address)
        elif not parsed.hostname:
            raise ValueError("No hostname in tcp address: " + address)

    else:
        raise ValueError("Unsupported scheme in: " + address)

    return parsed


def check_addresses(addresses, exit_on_fail=True,
                    exit_msg='ill-formatted addresses {{addresses}}: {{error}}'):
    '''
    Check the validity of addresses.
    :param addresses: iterable of str
        Addresses to check
    :param exit_on_fail: boolean
        Whether to exit when encountering an ill-formatted address
    :param exit_msg: str
        The message to print when exiting
    '''
    try:
        return [urlparse(address) for address in addresses]
    except ValueError as exc:
        if exit_on_fail:
            if exit_msg:
                print(exit_msg.format(addresses=addresses, error=str(exc)))
            sys.exit(1)
        else:
            raise


@functools.lru_cache(maxsize=1024)
def gethostbyname(hostname):
    return socket.gethostbyname(hostname)


@functools.lru_cache(maxsize=1)
def getlocalhostname():
    options = (socket.getfqdn, socket.gethostname, lambda: 'localhost')
    for option in options:
        try:
            address = option()
            gethostbyname(address)
            return address
        except Exception:
            pass
    return '127.0.0.1'


@functools.lru_cache(maxsize=1024)
def filter_ip_addresses(*addresses):
    '''
    Filter out IP addresses from a list of addresses. IP addresses are only
    selected from addresses with the tcp:// scheme.
    :param addresses: iterable of URL strings (parsable by urlparse)
    '''
    return set(
        gethostbyname(a.hostname)
        for a in map(urlparse, addresses)
        if a.scheme == 'tcp'
    )



class NotConnected(Exception):
    '''
    Raised when reading from or writing to a connection which is not / no
    longer connected.
    '''


class Connection(object):
    '''
    Connection object on top of an asyncio loop and a StreamReader and
    StreamWriter pair
    '''

    def __init__(self, loop, reader, writer):
        self.loop = loop
        self.reader = reader
        self.writer = writer
        self.read_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        self.bytes_received = 0
        self.bytes_sent = 0


    @property
    def is_connected(self):
        return not self.reader.at_eof() and not self.writer.transport._closing


    @asyncio.coroutine
    def close(self):
        with (yield from self.write_lock):
            self.writer.close()


    @asyncio.coroutine
    def send(self, msg, drain=True):
        '''
        Send a message
        :param msg: Message
            The message to send.
        :param drain: bool
            Whether to drain the socket after sending the message, defaults to
            True. If False there is no guarantee that the message will be sent
            unless more messages are sent (due to watermarks at the level of
            the asyncio transport)
        '''
        if not self.is_connected:
            raise NotConnected()
        logger.debug('sending %s', msg)
        marshalled, serialized, attachments = serialize.dump(msg)
        with (yield from self.write_lock):
            # send format header
            fmt = int(marshalled)
            fmt += int(bool(attachments)) * 2
            fmt = fmt.to_bytes(1, sys.byteorder)
            # send attachments, if any
            if attachments:
                # send attachment count
                self.writer.writelines((struct.pack('c', fmt), struct.pack('I', len(attachments))))
                for key, attachment in attachments.items():
                    with attachment() as (size, sender):
                        self.writer.writelines((struct.pack('I', len(key)), key, struct.pack('I', size)))
                        yield from async_call(self.loop, sender, self.writer)
                        self.bytes_sent += size
                self.writer.writelines((struct.pack('I', len(serialized)), serialized))
                self.bytes_sent += len(serialized)
            else:
                self.writer.writelines((struct.pack('c', fmt), struct.pack('I', len(serialized)), serialized))
                self.bytes_sent += len(serialized)

            if drain:
                yield from aio.drain(self.writer)
        logger.debug('sent %s', msg)


    def _recv_unpack(self, fmt, timeout=None):
        size = struct.calcsize(fmt)
        buffer = yield from asyncio.wait_for(self.reader.readexactly(size), timeout)
        return struct.unpack(fmt, buffer)[0]


    @asyncio.coroutine
    def recv(self, timeout=None):
        '''
        Receive a message from the connection.
        :param timeout: int or float
            timeout in seconds
        '''
        if not self.is_connected:
            raise NotConnected()
        try:
            with (yield from self.read_lock):
                # read and unpackformat
                fmt = yield from asyncio.wait_for(self.reader.readexactly(1), timeout)
                fmt = int.from_bytes(fmt, sys.byteorder)
                marshalled = fmt & 1
                has_attachments = fmt & 2

                # read in attachments if any
                attachments = {}
                if has_attachments:
                    att_count = yield from self._recv_unpack('I', timeout)
                    for _ in range(att_count):
                        # read key
                        keylen = yield from self._recv_unpack('I', timeout)
                        key = yield from asyncio.wait_for(self.reader.readexactly(keylen), timeout)
                        # read attachment
                        size = yield from self._recv_unpack('I', timeout)
                        attachment = yield from asyncio.wait_for(self.reader.readexactly(size), timeout)
                        attachments[key] = attachment
                        self.bytes_received += size

                # read message len and message
                length = yield from self._recv_unpack('I', timeout)
                msg = yield from asyncio.wait_for(self.reader.readexactly(length), timeout)
                self.bytes_received += length
            # parse the message and attachments
            return serialize.load(marshalled, msg, attachments)
        except BrokenPipeError as exc:
            raise NotConnected() from exc
        except asyncio.streams.IncompleteReadError as exc:
            if not exc.partial:
                raise NotConnected() from exc
            else:
                raise

    def __lt__(self, other):
        '''
        Compare this connection with another. Can be used to break ties.
        :param other: bndl.net.connection.Connection
        '''
        if other is None:
            # Works around a race condition in where two peer connections exist
            # and the 'tie' must be broken. In that case the peer connections
            # are 'compared'. The lower wins. However, since the tie breaking
            # happens on both ends, the peer may be disconnected and thus the
            # other peer.conn is None
            return True
        elif not isinstance(other, Connection):
            raise ValueError
        return min(self.sockname(), self.peername()) < min(other.sockname(), other.peername())


    def peername(self):
        peername = self.writer.get_extra_info('peername')
        if isinstance(peername, bytes):
            peername = peername.decode()
        return peername


    def sockname(self):
        sockname = self.writer.get_extra_info('sockname')
        if isinstance(sockname, bytes):
            sockname = sockname.decode()
        return sockname


    def socket(self):
        return self.writer.get_extra_info('socket')


    def socket_family(self):
        return self.socket().family


    def __repr__(self):
        return '<Connection %s %s>' % (
            '%s:%s' % self.sockname() + ' <-> ' + '%s:%s' % self.peername(),
            'connected' if self.is_connected else 'not connected'
        )
