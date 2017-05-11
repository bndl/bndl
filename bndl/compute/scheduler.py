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

from collections  import defaultdict, deque
from operator import attrgetter
from queue import Queue, Empty
from threading import Condition, RLock, Thread
import logging
import signal

from cytoolz import pluck
from sortedcontainers import SortedSet

from bndl.net.connection import NotConnected
from bndl.net.rmi import root_exc
from bndl.util.collection import sortgroupby, ensure_collection
from bndl.util.funcs import getter
from bndl.util.strings import fold_strings


logger = logging.getLogger(__name__)


EMPTY = ()


class DependenciesFailed(Exception):
    '''
    Indicate that a task failed due to dependencies not being 'available'. This will cause the
    dependencies to be re-executed and the task which raises DependenciesFailed will be scheduled
    to execute once the dependencies complete.

    The failures attribute is a nested mapping with the following structure:
    { result location : { executor name : [ids of tasks failed] } }
    '''
    def __init__(self, failures):
        self.failures = failures


class FailedDependency(Exception):
    '''
    Exception to be raised by task (i.e. returned from task.exception for tasks which have failed)
    to indicate that the task has been marked as failed post-hoc by the execution of another task.

    The executor_failed argument indicates whether the executor which executed the task should be
    considered lost or not.
    '''
    def __init__(self, executor_failed=None):
        self.executor_failed = executor_failed



class ExecutableOnDict(dict):
    def __init__(self, locality):
        self.locality = locality

    def __missing__(self, executor):
        def key(task):
            return (-self.locality[executor].get(task, 0), task.priority)
        return SortedSet(key=key)



class Scheduler():
    def __init__(self):
        # executors in FIFO order
        self.executors = {}
        self.executors_ready = deque()

        # running flag, set to False to stop
        self.running = False

        # serializes scheduler events and decisions
        self.lock = RLock()

        # condition triggered at task done / abort events
        self.condition = Condition(self.lock)

        # {task id -> (task, result_queue.put)}
        self.tasks = {}

        # {tasks sorted by priority}
        self.executable = SortedSet(key=attrgetter('priority'))

        # [tasks which are done (succeeded or failed) for processing by _task_done]
        self.done = deque()

        # {executor name -> {task -> locality}}
        self.locality = defaultdict(dict)

        # {executor name -> {tasks sorted by (-locality,priority)}
        self.executable_on = ExecutableOnDict(self.locality)

        # {executor name -> {forbidden tasks}}
        self.forbidden_on = defaultdict(set)

        # {executor name -> {tasks executing on executor}}
        self.executing_on = defaultdict(set)

        signal.signal(signal.SIGUSR2, lambda *args: self.debug())


    def debug(self, nudge=True):
        print('-' * 80)
        print('executable      :')
        if self.executable:
            print(fold_strings((t.id_str for t in self.executable), split='.'))
        print('done            :')
        if self.done:
            print(fold_strings((t.id_str for t in self.done), split='.'))
        print('executors_ready :')
        if self.executors_ready:
            print(fold_strings(map(str, self.executors_ready), split='.'))
        print('blocked         :')
        for task, _ in self.tasks.values():
            if task.blocked:
                print(task, '->', fold_strings((t.id_str for t in task.blocked), split='.'))
        print('executing on    :')
        for executor, executing_on in self.executing_on.items():
            if executing_on:
                print(executor, '->', fold_strings((t.id_str for t in executing_on), split='.'))
        print('forbidden on    :')
        for executor, forbidden_on in self.forbidden_on.items():
            if forbidden_on:
                print(executor, '->', fold_strings((t.id_str for t in forbidden_on), split='.'))
        print('lock            :', self.lock)
        print('tasks           :')
        for task in sorted((task for task, _ in self.tasks.values()), key=lambda t: t.priority):
            print(task)
        print('-' * 80)
        if nudge:
            with self.lock:
                self.condition.notify_all()


    def add_executor(self, executor):
        if executor.name in self.executors:
            return

        self.executors[executor.name] = executor

        with self.lock:
#             self._executor_maybe_ready(executor.name)
            self.executors_ready.append(executor.name)
            self.condition.notify_all()

            # self._drive_executor(executor)


    def remove_executor(self, executor):
        try:
            self.executors_ready.remove(executor.name)
        except ValueError:
            pass
        self.executors.pop(executor.name, None)
        self.executable_on.pop(executor.name, None)


    def start(self, name='bndl-scheduler-thread', daemon=True):
        Thread(target=self.run, name=name, daemon=daemon).start()


    def stop(self):
        if self.running:
            self.running = False
            with self.lock:
                self.condition.notify_all()


    def run(self):
        self.running = True

        try:
            while self.running:
                if (not self.executors_ready or not self.executable) and not self.done:
                    with self.lock:
                        if (not self.executors_ready or not self.executable) and not self.done:
                            self.condition.wait(5)

                if not self.running:
                    break

                while self.done:
                    self._task_done(self.done.popleft())

                if not self.executors_ready and not self.executing_on:
                    for task in self.executable:
                        task.mark_failed(RuntimeError('No executors available'))

                try:
                    executor_name = self.executors_ready.popleft()
                    executor = self.executors[executor_name]
                except (IndexError, KeyError):
                    continue

                if executor.is_connected:
                    executor_queue = self.executable_on[executor.name]
                    task = None

                    with self.lock:
                        # select a task for the executor
                        for t in list(executor_queue):
                            if t.started:
                                # task executed by another executor
                                executor_queue.remove(t)
                            elif t in self.executable and not t.blocked:
                                executor_queue.remove(t)
                                self.executable.remove(t)
                                task = t
                                break

                        if not task:
                            # no task available with locality > 0
                            # find task which is allowed to execute on this executor
                            forbidden_on = self.forbidden_on.get(executor_name, EMPTY)
                            for t in self.executable:
                                if t not in forbidden_on:
                                    self.executable.remove(t)
                                    task = t
                                    break

                        if task:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug('Executing %r on %r with locality %r',
                                             task, executor, self._get_locality(executor, task))
                            try:
                                self.executing_on[executor_name].add(task)
                                task.execute(self, executor)
                            except Exception as e:
                                task.mark_failed(e)
                        else:
                            self.executors_ready.append(executor_name)
        except Exception:
            logger.exception('Unexpected Scheduler error')
            raise
        finally:
            self.running = False


    def execute(self, tasks, order_results=True):
        assert self.running

        result_queue = Queue()

        logger.info('Executing %s tasks, %s executors available',
                    len(tasks), len(self.executors_ready))

        try:
            for task in tasks:
                for d in task.dependencies:
                    assert d in tasks
                for d in task.dependents:
                    assert d in tasks
                assert task.id not in self.tasks

                self.tasks[task.id] = (task, result_queue.put)
                task.add_listener(stopped=self._task_done_handoff)
                # task.add_listener(stopped=result_queue.put)

            with self.lock:
                for task in tasks:
                    blocked = any(not dep.succeeded for dep in task.dependencies)
                    if not task.succeeded and not blocked:
                        self._mark_executable(task)
                    if logger.isEnabledFor(logging.TRACE):
                        if task.succeeded:
                            logger.trace('%r is already done', task)
                        elif blocked:
                            logger.trace('%r is blocked', task)

            if order_results:
                yield from self._ordered_results(tasks, result_queue)
            else:
                yield from self._results(tasks, result_queue)

            logger.info('Executed %s tasks', len(tasks))
        finally:
            self.cancel_tasks(tasks)


    def cancel_tasks(self, tasks):
        exc = []
        with self.lock:
            for task in tasks:
                try:
                    if task.running:
                        logger.debug('Cancelling %r', task)
                        task.cancel()

                    task.release()
                except Exception as e:
                    exc.append((task, e))
                finally:
                    # TODO make this faster !!!
                    try:
                        self.tasks.pop(task.id)
                    except KeyError:
                        pass
                    else:
                        self.executable.discard(task)

                        for e in self.locality.values():
                            e.pop(task, None)

                        for e in self.forbidden_on.values():
                            e.discard(task)

                        for e in self.executable_on.values():
                            e.discard(task)

                        for executor, executing_on in self.executing_on.items():
                            try:
                                executing_on.remove(task)
                            except KeyError:
                                pass
                            else:
                                if not executing_on:
                                    self.executors_ready.append(executor)

        return exc


    def _results(self, tasks, results):
        remaining = set(t for t in tasks if not t.succeeded)
        while True:
            try:
                task = results.get(timeout=10)
            except Empty:
                print('FOR TESTING PURPOSES !!! 10 secs have passed since last result')
                self.debug(False)
                continue
            if not task.stopped:
                continue
            elif task.failed:
                raise task.exception

            try:
                remaining.remove(task)
            except KeyError:
                pass
            else:
                yield task

            if not remaining:
                break


    def _ordered_results(self, tasks, results):
        # keep a dict with results which are 'early' and the task id
        tasks = ensure_collection(tasks)
        task_indices = {t:i for i, t in enumerate(tasks)}
        next_task_idx = 0

        for task_idx, task in enumerate(tasks):
            if task.succeeded:
                next_task_idx = task_idx + 1
                yield task
            else:
                break

        if next_task_idx < len(tasks):
            for task in self._results(tasks, results):
                task_idx = task_indices[task]
                if task_idx == next_task_idx:
                    yield task
                    # possibly yield tasks which completed out of order
                    next_task_idx += 1
                    while next_task_idx < len(tasks):
                        task = tasks[next_task_idx]
                        if task.succeeded:
                            yield task
                        else:
                            break
                        next_task_idx += 1


    def _task_done_handoff(self, task):
        self.done.append(task)
        if self.lock.acquire(False):
            self.condition.notify_all()
            self.lock.release()


    def _task_done(self, task):
        if task.id not in self.tasks:
            logger.info('Unknown task %r signaled done', task)
            return

        executor_name = task.last_executed_on()
        executing_on = self.executing_on[executor_name]

        with self.lock:
            try:
                if task.succeeded:
                    self._task_succeeded(task)
                elif task.failed:
                    self._task_failed(task)
            except Exception as exc:
                logger.exception('Unable to handle task completion of %r', task)
                task.mark_failed(exc)

            try:
                executing_on.remove(task)
            except KeyError:
                pass
            else:
                assert executor_name not in self.executors_ready
                executor = self.executors.get(executor_name)
                if executor and executor.is_connected:
                    self.executors_ready.append(executor_name)
                    self.condition.notify_all()
                else:
                    logger.info('execution of %r stopped on %r, executor is %r',
                                task, executor_name, 'unknown' if not executor else 'not connected')


    def _emit(self, task):
        '''push out task to execute() thread'''
        try:
            return self.tasks[task.id][1](task)
        except KeyError:
            pass


    def _task_succeeded(self, task):
        assert task.succeeded, '%r not failed and not succeeded' % task

        executor = task.last_executed_on()
        logger.debug('%r succeeded on %r', task, executor)

        # check for unblocking of dependents
        for dependent in task.dependents:
            blocked = dependent.blocked
            if blocked:
                blocked.discard(task)

                if logger.isEnabledFor(logging.TRACE):
                    logger.trace(
                        '%r no longer blocks %r, %r remaining', task.id_str, dependent.id_str,
                        fold_strings((t.id_str for t in blocked), split='.') or 'none'
                    )

                if not blocked:
                    if dependent.succeeded:
                        self._task_done(dependent)
                    else:
                        self._mark_executable(dependent)

        self._emit(task)


    def _task_failed(self, task):
        assert task.failed, task

        # block tasks that dependent on it
        for dependent in task.dependents:
            self._mark_blocked(dependent, task)

        exc = root_exc(task.exception)

        if isinstance(exc, DependenciesFailed):
            logger.info('%r dependencies failed', task, exc_info=exc)
            self._dependencies_failed(task, exc)
        elif isinstance(exc, NotConnected):
            logger.info('%r failed on %r', task, task.last_executed_on(), exc_info=exc)
            self._transient_failure(task)
            try:
                self.executors_ready.remove(task.last_executed_on())
            except ValueError:
                pass
        elif isinstance(exc, FailedDependency):
            logger.debug('%r marked as failed by other task', task, exc_info=exc)
            self._transient_failure(task)
        else:
            self._general_failure(task, exc)


    def _dependencies_failed(self, task, exc):
        for result_loc, dependencies in exc.failures.items():
            for exc_loc, dependencies in dependencies.items():
                for dependency_id in dependencies:
                    if dependency_id not in self.tasks:
                        task.mark_failed(RuntimeError('Received DependenciesFailed for unknown'
                                                      'task with id %r', dependency_id))
                        return

                    dependency, _ = self.tasks[dependency_id]

                    if not dependency.failed and \
                       (not result_loc or result_loc == dependency.last_result_on()) and \
                       (not exc_loc or exc_loc == dependency.last_executed_on()):
                        dependency.mark_failed(FailedDependency(None))

        self._transient_failure(task)


    def _transient_failure(self, task):
        if not task.blocked:
            self._mark_executable(task)


    def _general_failure(self, task, exc):
        if task.attempts < task.max_attempts:
            if logger.isEnabledFor(logging.TRACE):
                logger.trace('%r allowed to restart (%s out of %s attempts)',
                             task, task.attempts, task.max_attempts, exc_info=exc)
            else:
                logger.debug('%r allowed to restart (%s out of %s attempts)',
                             task, task.attempts, task.max_attempts)
            self.forbidden_on[task.last_executed_on()].add(task)
            self._transient_failure(task)
        else:
            logger.info('%r failed on permanently %r', task, task.last_executed_on(), exc_info=exc)
            self._emit(task)


    def _mark_executable(self, task):
        if task in self.executable or task.running:
            return

        assert not task.blocked, '%r isn\'t executable because it is blocked'
        assert all(dep.succeeded for dep in task.dependencies), 'not all dependencies of %r succeeded: %r' \
            % (task, [dep for dep in task.dependencies if not dep.succeeded])

        logger.debug('%r is executable, %s executors available', task, len(self.executors_ready))

        if not task.succeeded:
            self.executable.add(task)

            executors = self.executors.values()
            localities = list(task.locality(executors) or ())
            for executor, locality in localities:
                executor_name = executor.name
                self.locality[executor_name][task] = locality
                if locality > 0 and not task.succeeded:
                    self.executable_on[executor_name].add(task)
                elif locality < 0:
                    self.forbidden_on[executor_name].add(task)

            if logger.isEnabledFor(logging.TRACE):
                for locality, group in sortgroupby(localities, getter(1)):
                    logger.trace('%r has locality %s on %s',
                                 task, locality,
                                 fold_strings((e.name for e in pluck(0, group)), split='.'))

        for dep in task.dependents:
            if not dep.succeeded:
                dep.blocked.add(task)

        self.condition.notify_all()


    def _mark_blocked(self, task, by):
        self.executable.discard(task)

        blocked = task.blocked

        if by not in blocked:
            logger.trace('%r is blocked by %r', task, by)

            if not blocked:
                for dep in task.dependents:
                    self._mark_blocked(dep, task)

            blocked.add(by)


    def _get_locality(self, executor, task):
        locality = self.locality.get(executor)
        if not locality:
            return 0
        else:
            return locality.get(task, 0)
