import asyncio
from asyncio.futures import CancelledError
from datetime import datetime
import logging

from bndl.net.messages import Ping
from random import random
from bndl.util.exceptions import catch


logger = logging.getLogger(__name__)


# The time in seconds between checking connections
WATCHDOG_INTERVAL = 2

# allow at most 10 connection attempts
# after that, drop the peer connection from the
# peer table
MAX_CONNECTION_ATTEMPT = 10

# The maximum time in seconds with no communication
# after which a ping is sent
DT_PING_AFTER = WATCHDOG_INTERVAL * 3

# The maximum time in seconds with no communication
# after which the connection is considered lost
DT_MAX_INACTIVE = DT_PING_AFTER * 2


class PeerStats(object):
    def __init__(self, peer):
        self.peer = peer
        self.connection_attempts = 0
        self.last_update = datetime.now()
        self.last_rx = None
        self.last_tx = None
        self.error_since = None

        self.bytes_sent = 0
        self.bytes_sent_rate = 0
        self.bytes_received = 0
        self.bytes_received_rate = 0


    def update(self):
        now = datetime.now()
        interval = (now - self.last_update).total_seconds()
        self.last_update = now

        if not self.peer.is_connected:
            self.error_since = self.error_since or now
            self.bytes_sent_rate = 0
            self.bytes_received_rate = 0
            return

        # calculate tx and rx rates
        self.bytes_sent_rate = (self.peer.conn.bytes_sent - self.bytes_sent) / interval
        self.bytes_sent = self.peer.conn.bytes_sent
        self.bytes_received_rate = (self.peer.conn.bytes_received - self.bytes_received) / interval
        self.bytes_received = self.peer.conn.bytes_received

        # mark rx activity
        if self.bytes_received_rate:
            self.last_rx = now
        if self.bytes_sent_rate:
            self.last_tx = now

        if self.last_rx and (datetime.now() - self.last_rx).total_seconds() > DT_MAX_INACTIVE:
            self.error_since = self.error_since or now
        else:
            # clear error stats
            self.connection_attempts = 0
            self.error_since = None


    def __str__(self):
        if self.error_since:
            fmt = '{peer.name} error since {error_since}'
        else:
            fmt = '{peer.name} communicating at {bytes_received_rate:.2f} kbps rx, {bytes_sent_rate} kbps tx'

        return fmt.format_map(self.__dict__)




class Watchdog(object):
    def __init__(self, node):
        self.node = node
        self.peer_stats = {}


    def start(self):
        self.monitor_task = self.node.loop.create_task(self._monitor())


    def stop(self):
        if self.monitor_task:
            self.monitor_task.cancel()
            self.monitor_task = None


    @asyncio.coroutine
    def _monitor(self):
        try:
            while self.node.running:
                yield from asyncio.sleep(WATCHDOG_INTERVAL)  # @UndefinedVariable
                try:
                    yield from self._check()
                except CancelledError:
                    raise
                except:
                    logger.exception('unable to check %s', self.node)
        except CancelledError:
            pass


    @asyncio.coroutine
    def _check(self):
        peers = list(self.node.peers.values())

        # check if a connection with a peer was dropped
        for peer in peers:
            stats = self.peer_stats.get(peer)
            if not stats:
                self.peer_stats[peer] = stats = PeerStats(peer)
            now = datetime.now()
            stats.update()

            if stats.connection_attempts > MAX_CONNECTION_ATTEMPT:
                popped = self.node.peers.pop(peer.name)
                if popped != peer:
                    self.node.peers[peer.name] = popped
                peer.disconnect('disconnected by watchdog after %s failed connection attempts', stats.connection_attempts)
                continue
            if stats.error_since and (now - stats.error_since).total_seconds() > (WATCHDOG_INTERVAL * 2 ** stats.connection_attempts * (random() / 2 + .75)):
                stats.connection_attempts += 1
                yield from peer.connect()
            elif stats.last_rx and (datetime.now() - stats.last_rx).total_seconds() > DT_PING_AFTER:
                with catch():
                    yield from peer.send(Ping())

        # if no nodes are connected, attempt to connect with the seeds
        if not any(peer.is_connected for peer in peers):
            yield from self.node._connect_seeds()


    def rxtx_stats(self):
        stats = dict(
            bytes_sent=0,
            bytes_sent_rate=0,
            bytes_received=0,
            bytes_received_rate=0
        )
        for peer_stats in self.peer_stats.values():
            for k in stats.keys():
                stats[k] += getattr(peer_stats, k, 0)
        return stats
