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
from concurrent.futures import Future
import atexit
import gc
import logging
import os
import queue
import threading
import time
import types
import weakref

from cytoolz import pluck
from psutil import virtual_memory, Process, NoSuchProcess
from sortedcontainers.sorteddict import SortedDict

from bndl.compute.storage import get_workdir
from bndl.net.connection import NotConnected
from bndl.net.rmi import Invocation
from bndl.util.conf import Float
from bndl.util.exceptions import catch
from bndl.util.funcs import getter
from bndl.util.psutil import process_memory_info
import bndl
from bndl.net.aio import get_loop
import asyncio


logger = logging.getLogger(__name__)


limit_system = Float(80, desc='The maximum amount of memory to be used by the system in percent (0-100).')
limit = Float(70, desc='The maximum amount of memory to be used by BNDL in percent (0-100).')


class MemoryCoordinator(object):
    def __init__(self, peers, limit=None, limit_system=None):
        self.limit = limit or bndl.conf['bndl.compute.memory.limit']
        self.limit_system = limit_system or bndl.conf['bndl.compute.memory.limit_system']

        self.memory_total = virtual_memory().total
        self.procs = {}
        self.lock = threading.Lock()
        self.running = False
        atexit.register(self.stop)

        peers.listeners.add(self.children_changed)


    def add_memory_manager(self, pid, release):
        if not isinstance(release, Invocation):
            # ensure local functions are performed asyncrhonously
            _release = release
            def _do_release(fut, nbytes):
                try:
                    fut.set_result(_release(nbytes))
                except Exception as exc:
                    fut.set_exception(exc)
            def release(nbytes):
                fut = Future()
                threading.Thread(target=_do_release, args=(fut, nbytes)).start()
                return fut
        try:
            proc = Process(pid)
        except NoSuchProcess:
            pass
        else:
            with self.lock:
                self.procs[pid] = (proc, release)


    def children_changed(self, event, peer):
        pid = peer.pid
        if event == 'added':
            self.add_memory_manager(pid, peer.service('memory').release)
        elif event == 'removed':
            with self.lock:
                del self.procs[pid]


    def get_procs(self):
        with self.lock:
            return list(self.procs.values())


    def stop(self):
        self.running = False


    def start(self):
        if self.running:
            return
        else:
            self.running = True
            self.loop = get_loop()
            self.loop.create_task(self.run())


    @asyncio.coroutine
    def run(self):
        while self.running:
            usage_system = virtual_memory().percent
            usage = yield from self.loop.run_in_executor(None, self.usage_pct)
            over = max(usage - self.limit, usage_system - self.limit_system)
            release = (over + 0.25 * self.limit) if over > 0 else 0

            if release > 0:
                release = release / 100 * self.memory_total
                logger.debug('Usage %.0f/%.0f (system usage %.0f/%.0f), requesting release of '
                             '%.0f mb', usage, self.limit, usage_system, self.limit_system,
                             release / 1024 / 1024)
                yield from self.loop.run_in_executor(None, self.release, release)

            yield from asyncio.sleep(.5, loop=self.loop)


    def release(self, nbytes):
        candidates = ((self.usage(proc), release) for proc, release in self.get_procs())
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
            except (NotConnected, RuntimeError):
                pass


    def usage(self, proc):
        try:
            rss = proc.memory_info().rss
            wd = get_workdir(False, proc.pid)
            for dirpath, _, filenames in  os.walk(wd, wd):
                try:
                    dir_fd = os.open(dirpath, os.O_RDONLY)
                    for filename in filenames:
                        rss += os.stat(filename, dir_fd=dir_fd).st_size
                except FileNotFoundError:
                    pass
                finally:
                    with catch():
                        os.close(dir_fd)
        except NoSuchProcess:
            with self.lock:
                del self.procs[proc.pid]
            return 0
        else:
            return rss


    def usage_pct(self):
        usage = 0
        for proc, _ in self.get_procs():
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
        did_release = False
        while self._halt_flag or self._release_queue:
            with self._condition:
                self._condition.wait_for(lambda: not self._halt_flag or self._release_queue)
                if self._release_queue:
                    try:
                        nbytes = self._release_queue.popleft()
                        released = self._release(nbytes)
                        did_release = True
                        logger.debug('Released %.0f mb', nbytes / 1024 / 1024)
                        self._released_queue.put(released)
                    except Exception:
                        logger.exception('Unable to release memory')
                        self._released_queue.put(0)
        return did_release



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



class LocalMemoryManager(object):
    def __init__(self):
        self.candidates = defaultdict(SortedDict)
        self._keys = {}
        self._candidates_lock = threading.Lock()
        self._requests = asyncio.Queue()
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
        self._loop.create_task(self._requests.put((time.time(), nbytes)))


    def start(self):
        if self._running:
            return
        else:
            self._running = True
            self._loop = get_loop()
            self._loop.create_task(self.run())


    def stop(self):
        if self._running:
            self._running = False
            self._loop.create_task(self._requests.put(None))


    @asyncio.coroutine
    def _get_request(self):
        while self._running:
            request = yield from self._requests.get()

            try:
                request = self._requests.get_nowait()
            except asyncio.QueueEmpty:
                break

        return request


    @asyncio.coroutine
    def run(self):
        while self._running:
            request = yield from self._get_request()
            if request is None:
                break

            req_time, nbytes = request

            if nbytes == 0 or req_time < self._last_release or not self.candidates:
                continue

            logger.debug('Executing release of %.0f mb', nbytes / 1024 / 1024)

            try:
                yield from self._loop.run_in_executor(None, self._release, nbytes)
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
            gc.collect(1)
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
