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

from _collections import defaultdict
from math import sqrt, log
import atexit
import logging
import queue
import threading
import time
import types
import weakref

from psutil import virtual_memory, Process, NoSuchProcess
from sortedcontainers.sorteddict import SortedDict
from toolz.itertoolz import pluck

from bndl.net.connection import NotConnected
from bndl.util.conf import Float
from bndl.util.funcs import getter
import bndl


logger = logging.getLogger(__name__)


limit_system = Float(90, desc='The maximum amount of memory to be used by the system in percent (0-100).')
limit = Float(80, desc='The maximum amount of memory to be used by BNDL in percent (0-100).')


class MemorySupervisor(threading.Thread):
    def __init__(self, supervisor_rmi, limit=None, limit_system=None):
        super().__init__(name='memory-monitor', daemon=True)

        self.limit = limit or bndl.conf['bndl.compute.memory.limit']
        self.limit_system = limit_system or bndl.conf['bndl.compute.memory.limit_system']

        supervisor_rmi.peers.listeners.add(self.children_changed)
        self.procs = {}
        self.lock = threading.Lock()

        self._total = virtual_memory().total

        self.running = False
        atexit.register(self.stop)


    def children_changed(self, event, peer):
        pid = int(peer.name.split('.')[-1])
        with self.lock:
            if event == 'added':
                proc = Process(pid)
                self.procs[pid] = (proc, peer.service('memory').release)
            elif event == 'removed':
                del self.procs[pid]


    def stop(self):
        self.running = False


    def run(self):
        self.running = True
        interval = .1
        rate = 0

        usage_prev = self.usage_pct()
        time_prev = time.time()

        while self.running:
            usage_system = virtual_memory().percent
            usage = self.usage_pct()

            over = max(usage - self.limit, usage_system - self.limit_system)
            release = over if over > 0 else 0

            now = time.time()
            rate = (usage - usage_prev) / (now - time_prev)
            time_prev = now

            rate_factor = 0
            if usage > self.limit * .75:
                rate_factor += usage / self.limit
            if usage_system > self.limit_system * .75:
                rate_factor += usage_system / self.limit_system

            if rate > 0 and rate_factor > 0:
                rate_release = rate * log(rate_factor * 2)
                release += rate_release

            if release > 0:
                logger.log(logging.INFO if over > 0 else logging.DEBUG,
                           'usage %.0f/%.0f (system usage %.0f/%.0f)',
                           usage, self.limit, usage_system, self.limit_system)

                release = release / 100 * self._total
                self.release(release)

                time.sleep(.1)
            else:
                self.release(0)
                interval_prev = interval
                if rate > 0:
                    interval = 1 / rate
                else:
                    interval = sqrt(-over) / 6
                if interval > interval_prev:
                    interval = (interval_prev + interval) / 2
                time.sleep(max(.1, min(interval, 1)))

            usage_prev = usage


    def release(self, nbytes):
        with self.lock:
            candidates = ((self.usage(proc), release) for proc, release in self.procs.values())
            candidates = sorted(candidates, key=getter(0), reverse=True)

        available = sum(pluck(0, candidates))
        if available:
            for usage, release in candidates:
                amount = max(0, usage / available * nbytes)
                try:
                    release(amount)
                except NotConnected:
                    pass


    def usage(self, proc):
        try:
            rss = proc.memory_info().rss
        except NoSuchProcess:
            with self.lock:
                del self.procs[proc.pid]
            return 0
        else:
            return rss


    def usage_pct(self):
        with self.lock:
            procs = list(self.procs.values())
        usage = 0
        for proc, _ in procs:
            try:
                usage += proc.memory_percent()
            except NoSuchProcess:
                pass
        return usage



class AsyncReleaseHelper(object):
    def __init__(self, key, manager, release):
        self._key = key
        self._manager = manager
        self._release = release
        self._halt_flag = False
        self._condition = None
        self._release_queue = None
        self._released_queue = None


    def __enter__(self):
        assert self._manager is not None
        self._condition = threading.Condition(threading.Lock())
        self._release_queue = []
        self._released_queue = queue.Queue()
        return self.check


    def __exit__(self, *args):
        self._manager.remove_releasable(self._key)
        self._manager = None
        self._released_queue.put(0)


    def halt(self):
        with self._condition:
            self._halt_flag = True
            self._condition.notify_all()


    def resume(self):
        with self._condition:
            self._halt_flag = False
            self._condition.notify_all()


    def release(self, nbytes):
        with self._condition:
            self._release_queue.append(nbytes)
            self._condition.notify_all()
        released = self._released_queue.get()
        self.resume()
        return released


    def check(self):
        if self._halt_flag:
            with self._condition:
                self._condition.wait_for(lambda: self._release_queue or not self._halt_flag)
                if self._release_queue:
                    nbytes = self._release_queue[-1]
                    self._release_queue.clear()
                    try:
                        released = self._release(nbytes)
                        logger.debug('Released %.0f mb', nbytes / 1024 / 1024)
                    except Exception:
                        logger.exception('Unable to release memory')
                        released = 0
                    self._released_queue.put(released)
                else:
                    self._released_queue.put(0)



class ReleaseReference(object):
    def __init__(self, method, callback):
        self.obj = weakref.ref(method.__self__, callback)
        self.func = method.__func__

    def release(self, *args):
        obj = self.obj()
        if obj is not None:
            return self.func(obj, *args)
        else:
            return 0


    @classmethod
    def wrap(cls, manager, key, release):
        if isinstance(release, types.MethodType):
            callback = lambda ref: manager.remove_releasable(key)
            return cls(release, callback).release
        else:
            return release



class LocalMemoryManager(threading.Thread):
    def __init__(self):
        super().__init__(name='bndl-local-memory-manager', daemon=True)
        self.candidates = defaultdict(SortedDict)
        self._keys = {}
        self._candidates_lock = threading.Lock()
        self._requests = queue.Queue()
        self._running = False


    def add_releasable(self, release, key, priority, size):
        release = ReleaseReference.wrap(self, key, release)

        with self._candidates_lock:
            tstamp = time.monotonic()
            self.candidates[priority][(tstamp, key)] = (release, size)
            self._keys[key] = (priority, tstamp, key)


    def remove_releasable(self, key):
        key = self._keys.pop(key, None)
        if key is not None:
            priority, tstamp, key = key
            with self._candidates_lock:
                self.candidates[priority].pop((tstamp, key), None)


    def async_release_helper(self, key, release, priority=0):
        helper = AsyncReleaseHelper(key, self, release)
        self.add_releasable(helper, key, priority, None)
        return helper


    def release(self, src, nbytes):
        self._requests.put(nbytes)


    def stop(self):
        self._running = False


    def run(self):
        self._running = True

        while self._running:
            while True:
                nbytes = self._requests.get()
                try:
                    nbytes = self._requests.get_nowait()
                except queue.Empty:
                    break

            if nbytes == 0:
                continue

            logger.debug('Executing release of %.0f mb', nbytes / 1024 / 1024)

            remaining = nbytes

            # snapshot the candidates across the priorities
            with self._candidates_lock:
                priorities = sorted(self.candidates.keys())
                candidates = {prio: list(self.candidates[prio].items())
                              for prio in priorities}

            # halt all async helpers at _all_ priorities
            for priority in priorities:
                for key, (candidate, size) in candidates[priority]:
                    if size is None:
                        candidate.halt()

            for priority in priorities:
                # release all fixed size candidates
                for key, (candidate, size) in candidates[priority]:
                    if size is not None:
                        candidate()
                        remaining -= size
                        with self._candidates_lock:
                            self._keys.pop(key[-1], None)
                            self.candidates[priority].pop(key, None)
                    if remaining <= 0:
                        break

                # release memory from / resume all async helpers
                for key, (candidate, size) in candidates[priority]:
                    if size is None:
                        # just resume of target reached
                        if remaining <= 0:
                            candidate.resume()
                        # or release from async helper if not
                        else:
                            remaining -= candidate.release(remaining)
