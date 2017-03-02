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
from datetime import datetime
from random import random
import asyncio
import atexit
import logging

from bndl.net.messages import Ping


logger = logging.getLogger(__name__)


# The time in seconds between checking connections
WATCHDOG_INTERVAL = 2

# allow at most 10 connection attempts
# after that, drop the peer connection from the
# peer table
MAX_CONNECTION_ATTEMPT = 10

# The maximum time in seconds with no communication
# after which a ping is sent
DT_PING_AFTER = 60

# The maximum time in seconds with no communication
# after which the connection is considered lost
DT_MAX_INACTIVE = DT_PING_AFTER * 2


class PeerStats(object):
    def __init__(self, peer):
        self.peer = peer
        self.connection_attempts = 0
        self.last_update = datetime.now()
        self.last_reconnect = None
        self.error_since = None

        self.bytes_sent = 0
        self.bytes_sent_rate = 0
        self.bytes_received = 0
        self.bytes_received_rate = 0


    def update(self):
        now = datetime.now()
        interval = (now - self.last_update).total_seconds()
        self.last_update = now

        if not self.peer.is_connected and self.peer.connected_on is not None:
            if not self.error_since:
                logger.info('%r disconnected', self.peer)
            self.error_since = self.error_since or now
            self.bytes_sent_rate = 0
            self.bytes_received_rate = 0
            return

        # calculate tx and rx rates
        if self.peer.is_connected:
            self.bytes_sent_rate = (self.peer.conn.bytes_sent - self.bytes_sent) / interval
            self.bytes_sent = self.peer.conn.bytes_sent
            self.bytes_received_rate = (self.peer.conn.bytes_received - self.bytes_received) / interval
            self.bytes_received = self.peer.conn.bytes_received

        if self.peer.last_rx and (now - self.peer.last_rx).total_seconds() > DT_MAX_INACTIVE:
            if not self.error_since:
                logger.info('%r is inactive for more than %s seconds (%s)', self.peer,
                            DT_MAX_INACTIVE, now - self.peer.last_rx)
            self.error_since = self.error_since or now
        else:
            if self.error_since:
                logger.info('%s recovered', self.peer)
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
        self._peer_stats = {}
        self.monitor_task = None

        atexit.register(self.stop)


    def start(self):
        self.monitor_task = self.node.loop.create_task(self._monitor())


    def stop(self):
        self.monitor_task = None


    def peer_stats(self, peer):
        stats = self._peer_stats.get(peer)
        if not stats:
            self._peer_stats[peer] = stats = PeerStats(peer)
        return stats


    @asyncio.coroutine
    def _monitor(self):
        try:
            while self.monitor_task and self.node.running:
                yield from self._check()
                yield from asyncio.sleep(WATCHDOG_INTERVAL, loop=self.node.loop)  # @UndefinedVariable
        except CancelledError:
            pass


    @asyncio.coroutine
    def _ping(self, peer):
        try:
            yield from peer.send(Ping())
        except Exception:
            self.peer_stats(peer).update()
            logger.warning('Unable to send ping to peer %r', peer, exc_info=True)


    @asyncio.coroutine
    def _check(self):
        for name in list(self.node.peers.keys()):
            try:
                # check a connection with a peer
                yield from self._check_peer(name)
            except CancelledError:
                raise
            except Exception:
                logger.exception('unable to check peer %s of %s', self.node.name, name)

        # if no nodes are connected, attempt to connect with the seeds
        if not any(peer.is_connected for peer in self.node.peers.values()):
            yield from self.node._connect_seeds()


    @asyncio.coroutine
    def _check_peer(self, name):
        try:
            peer = self.node.peers[name]
        except KeyError:
            return

        if peer.name != name:
            logger.info('Peer %s of node %s registered under %s, updating registration',
                           peer.name, self.node.name, name)
            peer = self.node.peers.pop(name)
            self.node.peers[name] = peer

        stats = self.peer_stats(peer)
        stats.update()

        if stats.connection_attempts > MAX_CONNECTION_ATTEMPT:
            popped = self.node.peers.pop(name)
            if popped != peer:
                self.node.peers[name] = popped
            yield from peer.disconnect('disconnected by watchdog after %s failed connection attempts',
                                       stats.connection_attempts)
        elif stats.error_since:
            # max reconnect interval is:
            # - twice the watch_dog interval (maybe something was missed)
            # - exponentially to the connection attempts (exponentially back off)
            # - with a random factor between 1 +/- .25
            now = datetime.now()
            connect_wait = WATCHDOG_INTERVAL * 2 ** stats.connection_attempts * (.75 + random() / 2)
            if (now - stats.error_since).total_seconds() > WATCHDOG_INTERVAL * 2 and \
               (not stats.last_reconnect or (now - stats.last_reconnect).total_seconds() > connect_wait):
                stats.connection_attempts += 1
                stats.last_reconnect = now
                yield from peer.connect()
        elif peer.is_connected and \
             peer.last_rx and \
             (datetime.now() - peer.last_rx).total_seconds() > DT_PING_AFTER:
            yield from self._ping(peer)


    def rxtx_stats(self):
        stats = dict(
            bytes_sent=0,
            bytes_sent_rate=0,
            bytes_received=0,
            bytes_received_rate=0
        )
        for peer_stats in self._peer_stats.values():
            for k in stats.keys():
                stats[k] += getattr(peer_stats, k, 0)
        return stats
