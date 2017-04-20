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

from collections  import defaultdict, deque, OrderedDict
from concurrent.futures import CancelledError
from threading import Condition, RLock
import logging

from sortedcontainers import SortedSet

from bndl.net.connection import NotConnected
from bndl.net.rmi import root_exc
from bndl.util.funcs import noop


logger = logging.getLogger(__name__)


class DependenciesFailed(Exception):
    '''
    Indicate that a task failed due to dependencies not being 'available'. This will cause the
    dependencies to be re-executed and the task which raises DependenciesFailed will be scheduled
    to execute once the dependencies complete.

    The failures attribute is a mapping from executor names (strings) to a sequence of
    task_ids which have failed.
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


class Scheduler(object):
    '''
    This scheduler executes Tasks taking into account their dependencies and executor locality.

    executor assignment takes into account:
     * concurrency (how many tasks must a executor execute concurrently)
     * and executor locality (0 is indifferent, -1 is forbidden, 1+ increasing locality)
       as locality 0 is likely to be common, this is assumed throughout the scheduler
       to reduce the memory cost for scheduling

    The most important component in the computational complexity of the scheduler is the number of
    dependencies to track. Many-to-many dependencies should be kept to the thousands or tens of
    thousands (i.e. 100 * 100 tasks). Such issues can be resolved by introducing a 'barrier task'
    as is done in bndl.compute (this reduced the number of dependencies to n+m instead of n*m).
    '''

    def __init__(self, tasks, done, executors, concurrency=1, attempts=1):
        '''
        Execute tasks in the given context and invoke done(task) when a task completes.

        :param tasks: iterable[task]
        :param done: callable(task)
            Invoked when a task completes. Must be thread safe. May be called multiple times
            if a task is reran (e.g. in case a executor fails). done(None) is called to signal
            completion of the last task.
        :param: executors: sequence[Peer]
            Sequence of executors to execute on.
        :param: concurrency: int (defaults to 1)
            @see: bndl.compute.concurrency
        :param: attempts: int (defaults to 1)
            @see: bndl.compute.attempts
        '''
        self.tasks = OrderedDict((task.id, task) for task
                                 in sorted(tasks, key=lambda t: t.priority))
        if len(self.tasks) == 0:
            raise ValueError('Tasks must provide at least one task to execute')
        if len(self.tasks) < len(tasks):
            raise ValueError('Tasks must have a unique task ID')

        for task in tasks:
            task.add_listener(noop, self.task_done)

        self.done = done
        self.executors = {executor.name:executor for executor in executors}

        if not self.executors:
            raise Exception('No executors available')

        self.concurrency = concurrency
        # failed tasks are retried on error, but they are executed at most attempts
        self.max_attempts = attempts

        # task completion is (may be) executed on another thread, this lock serializes access
        # on the containers below and executors_idle
        self.lock = RLock()
        # a condition is used to signal that a executor is available or the scheduler is aborted
        self.condition = Condition(self.lock)


    def run(self):
        logger.info('Executing job with %r tasks on %r executors', len(self.tasks), len(self.executors))

        self._abort = False
        self._exc = None

        # containers for states a task can be in
        self.executable = SortedSet(key=lambda task: task.priority)  # sorted executable tasks (sorted by task.id by default)
        self.blocked = defaultdict(set)  # blocked tasks task -> dependency ids executable or pending

        self.locality = {executor:{} for executor in self.executors.keys()}  # executor_name -> task -> locality > 0
        self.forbidden = defaultdict(set)  # task -> set[executor]
        # executor -> SortedList[task] in descending locality order
        self.executable_on = {executor:SortedSet(key=lambda task, executor=executor:-self.locality[executor].get(task, 0))
                              for executor in self.executors.keys()}

        self.pending = set()  # mapping of task -> executor for tasks which are currently in progress
        self.succeeded = set()  # tasks which have been executed successfully
        self.failures = defaultdict(int)  # failure counts per task (task -> int)

        # keep a FIFO queue of executors ready
        # and a list of idle executors (ready, but no more tasks to execute)
        self.executors_ready = deque(self.executors.keys())
        self.executors_idle = set()
        self.executors_failed = set()

        # perform scheduling under lock
        try:
            with self.lock:
                logger.debug('Calculating which tasks are executable, which are blocked and if there is locality')

                # create list of executable tasks and set of blocked tasks
                for task in self.tasks.values():
                    for executor, locality in task.locality(self.executors.values()) or ():
                        executor = executor.name
                        if locality < 0:
                            self.forbidden[task].add(executor)
                        elif locality > 0:
                            self.locality[executor][task] = locality
                            self.executable_on[executor].add(task)

                for task in self.tasks.values():
                    if task.succeeded:
                        self.succeeded.add(task)
                        self.done(task)
                    elif task.dependencies:
                        remaining = set(dep.id for dep in task.dependencies if not dep.succeeded)
                        if remaining:
                            self.blocked[task] = remaining
                        else:
                            self.executable.add(task)
                    else:
                        self.executable.add(task)

                if not self.executable:
                    raise Exception('No tasks executable (all tasks have dependencies)')
                if not self.executors_ready:
                    raise Exception('No executors available (all executors are forbidden by all tasks)')

                logger.debug('Starting %r tasks (%r tasks blocked) on %r executors (%r tasks already done)',
                             len(self.executable), len(self.blocked), len(self.executors_ready), len(self.succeeded))

                while True:
                    # wait for a executor to become available (signals task completion
                    self.condition.wait_for(lambda: self.executors_ready or self._abort)

                    if self._abort:
                        # the abort flag can be set to True to break the loop (in case of emergency)
                        for task in self.tasks.values():
                            if task in self.pending:
                                logger.debug('Job aborted, cancelling %r', task)
                                task.cancel()
                        break

                    executor = self.executors_ready.popleft()

                    if executor in self.executors_failed:
                        # the executor is 'ready' (a task was 'completed'), but with an error
                        # or the executor was marked as failed because another task depended on an output
                        # on this executor and the dependency failed
                        continue
                    elif not (self.executable or self.pending):
                        assert not any(1 for _ in filter(None, self.blocked.values()))
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug('No more tasks to execute or pending')
                        break
                    else:
                        task = self.select_task(executor)
                        if task:
                            # execute a task on the given executor and add the task_done callback
                            # the task is added to the pending set
                            try:
                                # assert task in self.executable, '%r is not executable' % task
                                # assert task not in self.succeeded, '%r already executed successfully' % task
                                # assert task not in self.pending, '%r already pending' % task
                                # assert not task.pending, '%r already pending' % task
                                # assert not task.done or task.failed, '%r done or failed' % task
                                # assert not self.blocked[task], '%r blocked' % task

                                # assert self.locality[executor].get(task, 0) >= 0, '%r forbidden on %r' % (task, executor)
                                # assert all(dep.succeeded for dep in task.dependencies), 'not all dependencies of %r succeeded' % task
                                # assert all(dep.id not in self.tasks or self.blocked[dep] for dep in task.dependents), \
                                #        'not all dependents of %r blocked' % task

                                self.executable.remove(task)
                                self.executable_on[executor].discard(task)
                                self.pending.add(task)
                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug('%r executing on %r with locality %r',
                                                 task, executor, self.locality[executor].get(task, 0))
                                task.execute(self, self.executors[executor])
                            except CancelledError:
                                pass
                            except AssertionError:
                                raise
                            except Exception as exc:
                                task.mark_failed(exc)
                        else:
                            self.executors_idle.add(executor)

        except Exception as exc:
            self._exc = exc

        if self._exc:
            logger.info('Failed after %r tasks with %s: %s',
                        len(self.succeeded), self._exc.__class__.__name__, self._exc)
            self.done(self._exc)
        elif self._abort:
            logger.info('Aborted after %r tasks', len(self.succeeded))
            self.done(Exception('Scheduler aborted'))
        else:
            logger.info('Completed %r tasks', len(self.succeeded))

        # always issue None (to facilitate e.g. iter(queue.get, None))
        self.done(None)


    def abort(self, exc=None):
        if exc is not None:
            self._exc = exc
        self._abort = True
        with self.lock:
            self.condition.notify_all()


    def select_task(self, executor):
        if not self.executable:
            return None

        # select a task for the executor
        executor_queue = self.executable_on[executor]
        for task in list(executor_queue):
            if task in self.pending or task in self.succeeded:
                # task executed by another executor
                executor_queue.remove(task)
            elif task in self.executable:
                return task
            elif self.blocked[task]:
                pass
            else:  # task not executable
                logger.error('%r not executable, blocked, pending nor executed', task)
                # assert False, '%r not executable, blocked, pending nor executed' % task

        # no task available with locality > 0
        # find task which is allowed to execute on this executor
        for task in self.executable:
            if executor not in self.forbidden[task]:
                return task


    def set_executable(self, task):
        if task.id not in self.tasks:
            return

        # assert not self.blocked[task], '%r isn\'t executable because it is blocked'
        # assert all(dep.succeeded for dep in task.dependencies), 'not all dependencies of %r succeeded: %r' \
        #     % (task, [dep for dep in task.dependencies if not dep.succeeded])
        # assert task not in self.succeeded, '%r already succeeded'

        if task in self.executable or task in self.pending or task.succeeded:
            return

        # calculate for each executor which tasks are forbidden or which have locality
        for executor in self.executors.keys():
            # don't bother with 'failed' executors
            if executor not in self.executors_failed:
                locality = self.locality[executor].get(task, 0)
                if locality >= 0:
                    # make sure the executor isn't 'stuck' in the idle set
                    if executor in self.executors_idle:
                        self.executors_idle.remove(executor)
                        for _ in range(self.concurrency):
                            self.executors_ready.append(executor)
                        self.condition.notify()

                    # the task has a preference for this executor
                    if locality > 0:
                        self.executable_on[executor].add(task)

        # check if there is a executor allowed to execute the task
        if len(self.forbidden[task]) == len(self.executors):
            raise Exception('%r cannot be executed on any available executors' % task)

        # add the task to the executable queue
        self.executable.add(task)


    def task_done(self, task):
        '''
        When a task completes, delete it from pending, add it to done
        and set dependent tasks as executable if this task was the last dependency.
        Reschedule failed tasks or abort scheduling if failed to often.
        '''
        if not task.done:
            return

        # nothing to do, scheduling was aborted
        if self._abort:
            return

        executor = task.last_executed_on()

        try:
            with self.lock:
                self.pending.discard(task)

                if task.failed:
                    self.task_failed(task)
                else:
                    # assert task.succeeded, '%r not failed and not succeeded' % task
                    # assert task not in self.succeeded, '%r completed while already in succeeded list' % task
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('%r was executed on %r', task, executor)
                    # add to executed and signal done
                    self.succeeded.add(task)
                    self.done(task)
                    # check for unblocking of dependents
                    for dependent in task.dependents:
                        blocked_by = self.blocked[dependent]
                        blocked_by.discard(task.id)
                        if not blocked_by and dependent:
                            if dependent in self.succeeded:
                                logger.trace('%r unblocked because %r was executed, '
                                             'but already succeeded', dependent, task)
                            else:
                                logger.debug('%r unblocked because %r was executed',
                                             dependent, task)
                                self.set_executable(dependent)

                if executor and executor not in self.executors_failed and \
                   not isinstance(task.exception(0), FailedDependency):
                    self.executors_ready.append(executor)
                    self.condition.notify()
        except Exception as exc:
            logger.exception('Unable to handle task completion of %r on %r',
                             task, executor)
            self.abort(exc)


    def task_failed(self, task):
        # in these cases we consider the task already re-scheduled
        if task in self.executable:
            logger.debug('%r failed with %s, but already marked as executable', task, type(root_exc(task.exception())).__name__)
            return
        elif task in self.pending:
            logger.debug('%r failed with %s, but already pending', task, type(root_exc(task.exception())))
            return

        # assert task.failed, "Can't reschedule task %r which hasn't failed." % task

        exc = root_exc(task.exception())

        if isinstance(exc, DependenciesFailed):
            deps = [(executor, dependencies)
                for dependencies in exc.failures.values()
                for executor, dependencies in dependencies.items()
            ]

            # assert task not in self.succeeded, 'Dependencies of %r failed which already completed successfully' % task
            if logger.isEnabledFor(logging.INFO):
                logger.info('%r failed on %s because %s dependencies failed, rescheduling',
                            task, task.last_executed_on(), len(deps))

            for executor, dependencies in deps:
                for task_id in dependencies:
                    try:
                        dependency = self.tasks[task_id]
                    except KeyError as e:
                        logger.error('Received DependenciesFailed for unknown task with id %r', task_id)
                        self.abort(e)
                    else:
                        # mark the executor as failed
                        last_executed_on = dependency.last_executed_on()
                        if not executor or executor == last_executed_on:
                            if executor and executor == last_executed_on:
                                logger.info('Marking %r as failed for dependency %s of %s',
                                            executor, dependency, task)
                                self.executors_failed.add(executor)
                                self.executors_idle.discard(executor)
                            if not dependency.failed:
                                dependency.mark_failed(FailedDependency(executor))
                        else:
                            # this should only occur with really really short tasks where the failure of a
                            # task noticed by task b is already obsolete because of the dependency was already
                            # restarted (because another task also issued DependenciesFailed)
                            logger.info('Received DependenciesFailed for task with id %r and executor %r '
                                        'but the task is last executed on %r',
                                         task_id, executor, last_executed_on)

        elif isinstance(exc, FailedDependency):
            self.succeeded.discard(task)
            executor = exc.executor_failed
            if executor:
                logger.info('%r marked as failed post-hoc, marking %r as failed', task, executor)
                self.executors_failed.add(executor)
                self.executors_idle.discard(executor)

        elif isinstance(exc, NotConnected):
            # mark the executor as failed
            logger.warning('%r failed with NotConnected, marking %r as failed',
                           task, task.last_executed_on())
            self.executors_failed.add(task.last_executed_on())

        else:
            self.failures[task] = failures = self.failures[task] + 1
            if failures >= self.max_attempts:
                logger.warning('%r failed on %r after %r attempts with %r ... aborting', task,
                               task.last_executed_on(), len(task.executed_on), task.exception())
                # signal done (failed) to allow bubbling up the error and abort
                self.done(task)
                self.abort(task.exception())
                return
            elif task.last_executed_on():
                logger.info('%r failed on %r with %s: %s, rescheduling',
                            task, task.last_executed_on(), exc.__class__.__name__, exc)
                self.forbidden[task].add(task.last_executed_on())
            else:
                logger.info('%r failed before being executed with %s: %s, rescheduling',
                            task, exc.__class__.__name__, exc)

        # block its dependencies
        for dependent in task.dependents:
            logger.debug('%r is blocked by failure of %r', dependent, task)
            self.blocked[dependent].add(task.id)
            self.executable.discard(dependent)

        if len(self.executors_failed) == len(self.executors):
            self.abort(Exception('Unable to complete job, all executors (%s) failed'
                                 % len(self.executors)))

        if not self.blocked[task] and task not in self.executable and task not in self.pending:
            self.set_executable(task)

        # assert self.blocked[task] or task in self.executable or task in self.pending
