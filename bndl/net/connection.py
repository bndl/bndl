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
import functools
import logging
import socket
import struct
import sys
import types
import urllib.parse

from bndl.net import aio
from bndl.net import serialize
from bndl.util.conf import Bool
import bndl


nodelay = Bool(True, desc='Whether to set TCP_NODELAY on bndl.net.connection.Connection objects')


logger = logging.getLogger(__name__)


def urlparse(address):
    '''
    Parse an address with urllib.parse.urlparse and checking validity in the
    context of bndl.

    Args:
        address (str): The address to parse
    '''
    if '://' not in address:
        address = 'tcp://' + address

    parsed = urllib.parse.urlparse(address)

    if parsed.scheme == 'tcp':
        if parsed.path:
            raise ValueError('Illegal url: "%s", path not supported in tcp address (%s)' % (address, parsed.path))
        elif not parsed.hostname:
            raise ValueError('Illegal url: "%s", no hostname in tcp address: ' % address)
    else:
        raise ValueError('Illegal url: "%s", unsupported scheme "%s"' % (address, parsed.scheme))

    if parsed.port is None:
        components = list(parsed)
        if not components[1].endswith(':'):
            components[1] += ':'
        components[1] += "5000"
        with_port = urllib.parse.urlunparse(components)
        return urllib.parse.urlparse(with_port)

    return parsed


def urlcheck(address):
    return urlparse(address).geturl()


@functools.lru_cache(maxsize=1024)
def gethostbyname(hostname):
    '''A cached version of `socket.gethostbyname`'''
    return socket.gethostbyname(hostname)


@functools.lru_cache(maxsize=1)
def getlocalhostname():
    '''
    Provides a local hostname using :meth:`gethostbyname` by attempting to get it from

     * `gethostbyname(socket.getfqdn())`
     * `gethostbyname(socket.gethostname())`
     * `gethostbyname('localhost')`

    If the above options fail, this method falls back to 127.0.0.1.
    '''
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

    Args:
        *addresses (iterable): URL strings (parsable by urlparse)
    '''
    return frozenset(
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
        self.readexactly = types.MethodType(aio.readexactly, self.reader)
        self.writer = writer
        self.write_lock = asyncio.Lock(loop=self.loop)
        self.bytes_received = 0
        self.bytes_sent = 0

        sock = self.socket()
        if sock and sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,
                            bndl.conf['bndl.net.connection.nodelay'])


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
        Send a message on this connection.

        Args:
            msg (:class:`bndl.net.messages.Message`): The message to send.
            drain (bool): Whether to drain the socket after sending the message, defaults to True.
            If False there is no guarantee that the message will be sent unless more messages are
            sent (due to watermarks at the level of the asyncio transport)
        '''
        if not self.is_connected:
            raise NotConnected()
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
                    with attachment(self.loop, self.writer) as (size, sender):
                        self.writer.writelines((struct.pack('I', len(key)), key, struct.pack('Q', size)))
                        try:
                            if asyncio.iscoroutinefunction(sender):
                                yield from sender()
                            else:
                                sender()
                        except Exception as e:
                            raise RuntimeError('Unable to send attachment %r : %r' %
                                               (key, attachment)) from e
                        self.bytes_sent += size
                self.writer.writelines((struct.pack('Q', len(serialized)), serialized))
                self.bytes_sent += len(serialized)
            else:
                self.writer.writelines((struct.pack('c', fmt), struct.pack('Q', len(serialized)), serialized))
                self.bytes_sent += len(serialized)

            if drain:
                yield from aio.drain(self.writer)


    @asyncio.coroutine
    def _recv_unpack(self, fmt):
        size = struct.calcsize(fmt)
        buffer = yield from self.readexactly(size)
        self.bytes_received += size
        return struct.unpack(fmt, buffer)[0]


    @asyncio.coroutine
    def _recv_field(self, size_fmt='I'):
        frame_len = yield from self._recv_unpack(size_fmt)
        frame = yield from self.readexactly(frame_len)
        self.bytes_received += frame_len
        return frame


    @asyncio.coroutine
    def _recv(self):
        # read and unpack format
        fmt = yield from self.readexactly(1)
        fmt = int.from_bytes(fmt, sys.byteorder)
        marshalled = fmt & 1
        has_attachments = fmt & 2

        # read in attachments if any
        attachments = {}
        if has_attachments:
            att_count = yield from self._recv_unpack('I')
            for _ in range(att_count):
                key = bytes((yield from self._recv_field()))
                attachments[key] = yield from self._recv_field('Q')

        # read message itself
        msg = yield from self._recv_field('Q')

        return marshalled, msg, attachments


    @asyncio.coroutine
    def recv(self, timeout=None):
        '''
        Receive a message from the connection.

        Args:
            timeout (float): timeout in seconds
        '''
        try:
            payload = yield from asyncio.wait_for(self._recv(), timeout, loop=self.loop)
            return serialize.load(*payload)
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
        try:
            desc = '%s:%s' % self.sockname()[:2] + ' <-> ' + '%s:%s' % self.peername()[:2]
        except Exception:
            desc = '%s, %s' % (self.reader, self.writer)
        return '<Connection %s %s>' % (
            desc,
            'connected' if self.is_connected else 'not connected'
        )
