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

import concurrent.futures
import os
import sys
import textwrap
import threading
import traceback

from bndl.util.exceptions import catch


class OnDemandThreadedExecutor(concurrent.futures.Executor):
    '''
    An minimal - almost primitive - Executor, that spawns a thread per task.

    Used only because concurrent.futures.ThreadPoolExecutor isn't able to
    scale down the number of active threads, deciding on a maximum number of
    concurrent tasks may be difficult and keeping max(concurrent tasks) threads
    lingering around seems wasteful.

    TODO: replace with a proper executor with min and max threads, etc.
    '''

    def submit(self, fn, *args, **kwargs):
        future = concurrent.futures.Future()
        def work():
            try:
                result = fn(*args, **kwargs)
                future.set_result(result)
            except Exception as exception:
                future.set_exception(exception)
        threading.Thread(target=work).start()
        return future


class Coordinator(object):
    '''
    The Coordinator class coordinates threads which are interested in getting
    some work done (a function called) but it needs to be done only once.
    '''
    def __init__(self, lock=None):
        self._lock = lock or threading.RLock()
        self.acquire = self._lock.acquire
        self.release = self._lock.release
        self._done = {}
        self._results = {}


    def __getitem__(self, key, value):
        return self._results[key]

    def __setitem__(self, key, value):
        self._results[key] = value
        with self._lock:
            try:
                done = self._done[key]
            except KeyError:
                done = threading.Event()
                self._done[key] = done
        done.set()


    def __delitem__(self, key):
        self.clear(key)


    def clear(self, key):
        '''
        Clear any state (progress flags and results) for key.
        '''
        with self._lock:
            with catch(KeyError):
                del self._done[key]
            with catch(KeyError):
                del self._results[key]


    def coordinate(self, work, key):
        '''
        Coordinate with other threads that work is called only once and it's
        result is available.
        :param work: function
            A function to coordinate the invocation of across threads.
        :param key: hashable obj
            Work with the same key will be coordinated.
        '''
        with self._lock:
            try:
                # short path for one result is available
                return self._results[key]
            except KeyError:
                # setup for longer path to coordinate work
                try:
                    done = self._done[key]
                    wait = True
                except KeyError:
                    done = threading.Event()
                    self._done[key] = done
                    wait = False
        if wait:
            # wait for another thread to do the work
            done.wait()
            return self._results[key]
        else:
            # do the work in the current thread
            self._results[key] = result = work()
            # and notify (future) other threads
            done.set()
            return result


def dump_threads(*args, **kwargs):
    threads = list(threading.enumerate())
    frames = sys._current_frames()
    print('Threads (%s) of process %s' % (len(threads), os.getpid()))
    for idx, thread in enumerate(threads):
        print(' %s id=%s name=%s (%s%s)' % (idx, thread.ident, thread.name, type(thread).__name__,
                                            (', daemon' if thread.daemon else '')))
        stack = ''.join(traceback.format_stack(frames[thread.ident]))
        print(textwrap.indent(stack, '   '))
