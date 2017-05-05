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
        self._waiters = {}


    def coordinate(self, work, key):
        '''
        Coordinate with other threads that work is called only once and it's
        result is available.

        :param work: function
            A function to be invoked once across threads.
        :param key: hashable obj
            Work with the same key will be coordinated.
        '''
        with self._lock:
            waiters = self._waiters.get(key)
            if waiters is None:
                waiters = self._waiters[key] = []
            else:
                future = concurrent.futures.Future()
                waiters.append(future)
                return future.result()

        try:
            result = work()
            with self._lock:
                for waiter in self._waiters.pop(key, ()):
                    waiter.set_result(result)
            return result
        except Exception as e:
            with self._lock:
                for waiter in self._waiters.pop(key, ()):
                    waiter.set_exception(e)
            raise


def match_thread(stack, end):
    for frame, (file, func) in zip(stack[-len(end):], end):
        if not frame[0].endswith(file) or frame[2] != func:
            return False
    return True


TPE_WORKER_TAIL = (
    ('threading.py', 'run'),
    ('concurrent/futures/thread.py', '_worker'),
    ('queue.py', 'get'),
    ('threading.py', 'wait'),
)


def dump_threads(*args, **kwargs):
    threads = list(threading.enumerate())
    frames = sys._current_frames()

    tpe_workers = 0

    print('Threads (%s) of process %s' % (len(threads), os.getpid()))
    for idx, thread in enumerate(threads, 1):
        try:
            stack = frames[thread.ident]
        except KeyError:
            pass
        else:
            stack = traceback.extract_stack(stack)
            if len(stack) >= 4 and match_thread(stack, TPE_WORKER_TAIL):
                tpe_workers += 1
            else:
                print(' %s id=%s name=%s (%s%s)' %
                      (idx, thread.ident, thread.name, type(thread).__name__,
                       (', daemon' if thread.daemon else '')))
                stack = ''.join(traceback.format_list(stack))
                print(textwrap.indent(stack, ' '))

    if tpe_workers:
        print(' Idle ThreadPoolExecutor workers: %s' % tpe_workers)
        print()
