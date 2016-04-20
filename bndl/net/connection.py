import asyncio
import logging
import socket
import struct
import sys
import urllib.parse

from bndl.util import serialize
from bndl.net.messages import Message


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

    elif parsed.scheme == 'unix':
        if not parsed.path:
            raise ValueError("No path in unix address: " + address)

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
    except ValueError as e:
        if exit_on_fail:
            if exit_msg:
                print(exit_msg.format(addresses=addresses, error=str(e)))
            sys.exit(1)
        else:
            raise e


def filter_ip_addresses(addresses):
    '''
    Filter out IP addresses from a list of addresses. IP addresses are only
    selected from addresses with the tcp:// scheme.
    :param addresses: iterable of URL strings (parsable by urlparse)
    '''
    return (
        socket.gethostbyname(a.hostname)
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


    @property
    def is_connected(self):
        return not self.reader.at_eof() and not self.writer.transport._closing


    def close(self):
        with (yield from self.write_lock):
            self.writer.close()


    @asyncio.coroutine
    def send(self, msg, drain=False):
        '''
        Send an object
        :param msg: Message
            The message to send.
        :param drain:
        '''
        if not self.is_connected:
            raise NotConnected()
        # TODO investigate alternatives to pickling
        logger.debug('sending %s', msg)
        fmt, serialized = (yield from self.loop.run_in_executor(None, serialize.dumps, msg.__msgdict__()))
        with (yield from self.write_lock):
            self.writer.writelines((struct.pack('I', len(serialized)), struct.pack('c', fmt), serialized))
            if drain:
                yield from self.writer.drain()
        logger.debug('sent %s', msg)


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
                header = yield from asyncio.wait_for(self.reader.readexactly(5), timeout)
                l = struct.unpack('I', header[:4])[0]
                fmt = struct.unpack('c', header[4:])[0]
                msg = yield from asyncio.wait_for(self.reader.readexactly(l), timeout)
            msg = (yield from self.loop.run_in_executor(None, serialize.loads, fmt, msg))
            msg = Message.load(msg)
            return msg
        except asyncio.streams.IncompleteReadError as e:
            if not e.partial:
                raise NotConnected() from e
            else:
                raise e

    def __lt__(self, other):
        '''
        Compare this connection with another. Can be used to break ties.
        :param other: bndl.net.connection.Connection
        '''
        if not isinstance(other, Connection):
            raise ValueError()

        if self.socket_family() != socket.AF_UNIX and other.socket_family() != socket.AF_UNIX:
            return min(self.sockname(), self.peername()) < min(other.sockname(), other.peername())
        if self.socket_family() == socket.AF_UNIX and other.socket_family() == socket.AF_UNIX:
            return (self.sockname() or self.peername()) < (other.sockname() or other.peername())
        elif self.socket_family() == socket.AF_UNIX:
            return True
        else:
            return False


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


    def __str__(self):
        if self.socket_family() == socket.AF_UNIX:
            local = self.sockname()
            if local:
                return 'UDS listening on ' + local
            remote = self.peername()
            if remote:
                return 'UDS connected to ' + remote
        else:
            return '%s:%s' % self.sockname() + ' <-> ' + '%s:%s' % self.peername()

