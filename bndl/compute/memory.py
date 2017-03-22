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

from collections import defaultdict, deque
import atexit
import gc
import logging
import queue
import threading
import time
import types
import weakref

from cytoolz import pluck
from psutil import virtual_memory, Process, NoSuchProcess
from sortedcontainers.sorteddict import SortedDict

from bndl.net.connection import NotConnected
from bndl.util.conf import Float
from bndl.util.funcs import getter
from bndl.util.psutil import process_memory_info
import bndl


logger = logging.getLogger(__name__)


limit_system = Float(70, desc='The maximum amount of memory to be used by the system in percent (0-100).')
limit = Float(60, desc='The maximum amount of memory to be used by BNDL in percent (0-100).')


class MemoryCoordinator(threading.Thread):
    def __init__(self, peers, limit=None, limit_system=None):
        super().__init__(name='memory-monitor', daemon=True)

        self.limit = limit or bndl.conf['bndl.compute.memory.limit']
        self.limit_system = limit_system or bndl.conf['bndl.compute.memory.limit_system']

        peers.listeners.add(self.children_changed)
        self.procs = {}
        self.lock = threading.Lock()

        self._total = virtual_memory().total

        self.running = False
        atexit.register(self.stop)


    def children_changed(self, event, peer):
        pid = int(peer.name.split('.')[-1])
        with self.lock:
            if event == 'added':
                try:
                    proc = Process(pid)
                except NoSuchProcess:
                    pass
                else:
                    self.procs[pid] = (proc, peer.service('memory').release)
            elif event == 'removed':
                del self.procs[pid]


    def stop(self):
        self.running = False


    def run(self):
        self.running = True

        while self.running:
            usage_system = virtual_memory().percent
            usage = self.usage_pct()

            over = max(usage - self.limit, usage_system - self.limit_system)
            release = (over + 0.25 * self.limit) if over > 0 else 0

            if release > 0:
                release = release / 100 * self._total

                logger.debug('Usage %.0f/%.0f (system usage %.0f/%.0f), requesting release of '
                             '%.0f mb', usage, self.limit, usage_system, self.limit_system,
                             release / 1024 / 1024)

                self.release(release)

            time.sleep(.5)


    def release(self, nbytes):
        with self.lock:
            candidates = list(self.procs.values())

        candidates = ((self.usage(proc), release) for proc, release in candidates)
        candidates = sorted(candidates, key=getter(0), reverse=True)
        ncandidates = len(candidates)

        if ncandidates < 1:
            return

        remaining = nbytes
        amounts = [0] * ncandidates

        for idx, usage in enumerate(pluck(0, candidates)):
            amount = usage / 4 * 3
            amounts[idx] = amount
            remaining = max(0, remaining - amount)
            if remaining == 0:
                break

        for amount, (usage, release) in zip(amounts, candidates):
            amount2 = min(usage, amount + remaining)
            remaining -= amount2 - amount
            try:
                release(amount2)
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
        self._halt_flag = False
        self._condition = threading.Condition()
        self._release_queue = deque()
        self._released_queue = queue.Queue()
        return self.check


    def __exit__(self, *args):
        self._manager.remove_releasable(self._key)
        self._manager = None
        self._released_queue.put(0)
        self._halt_flag = False


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
        return self._released_queue.get()


    def check(self):
        while self._halt_flag or self._release_queue:
            with self._condition:
                self._condition.wait_for(lambda: not self._halt_flag or self._release_queue)
                if self._release_queue:
                    try:
                        nbytes = self._release_queue.popleft()
                        released = self._release(nbytes)
                        logger.debug('Released %.0f mb', nbytes / 1024 / 1024)
                        self._released_queue.put(released)
                    except Exception:
                        logger.exception('Unable to release memory')
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
        super().__init__(name='local-memory-manager', daemon=True)
        self.candidates = defaultdict(SortedDict)
        self._keys = {}
        self._candidates_lock = threading.Lock()
        self._requests = queue.Queue()
        self._running = False
        self._last_release = time.time()


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
        self._requests.put((time.time(), nbytes))


    def stop(self):
        self._running = False
        self._requests.put(None)


    def _get_request(self):
        while self._running:
            request = self._requests.get()

            try:
                request = self._requests.get_nowait()
            except queue.Empty:
                break

        return request


    def run(self):
        self._running = True

        while self._running:
            request = self._get_request()
            if request is None:
                break

            req_time, nbytes = request

            if nbytes == 0 or req_time < self._last_release or not self.candidates:
                continue

            logger.debug('Executing release of %.0f mb', nbytes / 1024 / 1024)

            try:
                self._release(nbytes)
                self._last_release = time.time()
            except Exception:
                logger.exception('Unable to release memory')


    def _release(self, nbytes):
        target = process_memory_info().rss - nbytes
        done_flag = False

        def remaining():
            return process_memory_info().rss - target

        def done():
            nonlocal done_flag
            gc.collect()
            done_flag = done_flag or (remaining() <= 0)
            return done_flag

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
                    with self._candidates_lock:
                        self._keys.pop(key[-1], None)
                        self.candidates[priority].pop(key, None)
                if done():
                    break

            # release memory from / resume all async helpers
            for key, (candidate, size) in candidates[priority]:
                if size is None:
                    if not done():
                        candidate.release(remaining())

        logger.info(
            'Released %.0f of %.0f mb (usage %.0f to %.0f mb)',
            ((target + nbytes) - process_memory_info().rss) / 1024 / 1024,
            nbytes / 1024 / 1024,
            (target + nbytes) / 1024 / 1024,
            process_memory_info().rss / 1024 / 1024
        )

        # resume all async helpers at _all_ priorities
        for priority in priorities:
            for key, (candidate, size) in candidates[priority]:
                if size is None:
                    candidate.resume()
