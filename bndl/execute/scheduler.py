from collections  import defaultdict, deque, OrderedDict
from concurrent.futures import CancelledError
from threading import Condition, RLock
import logging

from bndl.execute import DependenciesFailed
from bndl.net.connection import NotConnected
from bndl.rmi import root_exc
from bndl.util.funcs import noop
from sortedcontainers import SortedSet


logger = logging.getLogger(__name__)


class FailedDependency(Exception):
    '''
    Exception to be raised by task (i.e. returned from task.exception for tasks which have failed)
    to indicate that the task has been marked as failed post-hoc by the execution of another task.

    The worker_failed argument indicates whether the worker which executed the task should be
    considered lost or not.
    '''
    def __init__(self, worker_failed=None):
        self.worker_failed = worker_failed


class Scheduler(object):
    '''
    This scheduler executes Tasks taking into account their dependencies and worker locality.

    Worker assignment takes into account:
    - concurrency (how many tasks must a worker execute concurrently)
    - and worker locality (0 is indifferent, -1 is forbidden, 1+ increasing locality)
      as locality 0 is likely to be common, this is assumed throughout the scheduler
      to reduce the memory cost for scheduling

    The most important component in the computational complexity of the scheduler is the number of
    dependencies to track. Many-to-many dependencies should be kept to the thousands or tens of
    thousands (i.e. 100 * 100 tasks). Such issues can be resolved by introducing a 'barrier task'
    as is done in bndl.compute (this reduced the number of dependencies to n+m instead of n*m).
    '''

    def __init__(self, tasks, done, workers, concurrency=1, attempts=1):
        '''
        Execute tasks in the given context and invoke done(task) when a task completes.
        
        :param tasks: iterable[task]
        :param done: callable(task)
            Invoked when a task completes. Must be thread safe. May be called multiple times
            if a task is reran (e.g. in case a worker fails). done(None) is called to signal
            completion of the last task.
        :param: workers: sequence[Peer]
            Sequence of workers to execute on.
        :param: concurrency: int (defaults to 1)
            @see: bndl.execute.concurrency
        :param: attempts: int (defaults to 1)
            @see: bndl.execute.attempts
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
        self.workers = {worker.name:worker for worker in workers}

        if not self.workers:
            raise Exception('No workers available')

        self.concurrency = concurrency
        # failed tasks are retried on error, but they are executed at most attempts
        self.max_attempts = attempts

        # task completion is (may be) executed on another thread, this lock serializes access
        # on the containers below and workers_idle
        self.lock = RLock()
        # a condition is used to signal that a worker is available or the scheduler is aborted
        self.condition = Condition(self.lock)


    def run(self):
        logger.info('Executing job with %r tasks on %r workers', len(self.tasks), len(self.workers))

        self._abort = False
        self._exc = None

        # containers for states a task can be in
        self.executable = SortedSet(key=lambda task: task.priority)  # sorted executable tasks (sorted by task.id by default)
        self.blocked = defaultdict(set)  # blocked tasks task -> dependencies executable or pending

        self.locality = {worker:{} for worker in self.workers.keys()}  # worker_name -> task -> locality > 0
        self.forbidden = defaultdict(set)  # task -> set[worker]
        # worker -> SortedList[task] in descending locality order
        self.executable_on = {worker:SortedSet(key=lambda task, worker=worker:-self.locality[worker].get(task, 0))
                              for worker in self.workers.keys()}

        self.pending = set()  # mapping of task -> worker for tasks which are currently in progress
        self.succeeded = set()  # tasks which have been executed successfully
        self.failures = defaultdict(int)  # failure counts per task (task -> int)

        # keep a FIFO queue of workers ready
        # and a list of idle workers (ready, but no more tasks to execute)
        self.workers_ready = deque(self.workers.keys())
        self.workers_idle = set()
        self.workers_failed = set()

        # perform scheduling under lock
        try:
            with self.lock:
                logger.debug('Calculating which tasks are executable, which are blocked and if there is locality')

                # create list of executable tasks and set of blocked tasks
                for task in self.tasks.values():
                    for worker, locality in task.locality(self.workers.values()) or ():
                        worker = worker.name
                        if locality < 0:
                            self.forbidden[task].add(worker)
                        elif locality > 0:
                            self.locality[worker][task] = locality
                            self.executable_on[worker].add(task)

                for task in self.tasks.values():
                    if task.succeeded:
                        self.succeeded.add(task)
                        self.done(task)
                    elif task.dependencies:
                        remaining = set(dep for dep in task.dependencies if not dep.succeeded)
                        if remaining:
                            self.blocked[task] = remaining
                        else:
                            self.executable.add(task)
                    else:
                        self.executable.add(task)

                if not self.executable:
                    raise Exception('No tasks executable (all tasks have dependencies)')
                if not self.workers_ready:
                    raise Exception('No workers available (all workers are forbidden by all tasks)')

                logger.debug('Starting %r tasks (%r tasks blocked) on %r workers (%r tasks already done)',
                             len(self.executable), len(self.blocked), len(self.workers_ready), len(self.succeeded))

                while True:
                    # wait for a worker to become available (signals task completion
                    self.condition.wait_for(lambda: self.workers_ready or self._abort)

                    if self._abort:
                        # the abort flag can be set to True to break the loop (in case of emergency)
                        for task in self.tasks.values():
                            if task in self.pending:
                                task.cancel()
                        break

                    worker = self.workers_ready.popleft()

                    if worker in self.workers_failed:
                        # the worker is 'ready' (a task was 'completed'), but with an error
                        # or the worker was marked as failed because another task depended on an output
                        # on this worker and the dependency failed
                        continue
                    elif not (self.executable or self.pending):
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug('No more tasks to execute or pending (%r tasks blocked)',
                                         sum(1 for _ in filter(None, self.blocked.values())))
                        break
                    else:
                        task = self.select_task(worker)
                        if task:
                            # execute a task on the given worker and add the task_done callback
                            # the task is added to the pending set
                            try:
                                assert task in self.executable, '%r is not executable' % task
                                assert task not in self.succeeded, '%r already executed successfully' % task
                                assert task not in self.pending, '%r already pending' % task
                                assert not task.pending, '%r already pending' % task
                                assert not task.done or task.failed, '%r done or failed' % task
                                assert not self.blocked[task], '%r blocked' % task

                                assert self.locality[worker].get(task, 0) >= 0, '%r forbidden on %r' % (task, worker)
                                assert all(dep.succeeded for dep in task.dependencies), 'not all dependencies of %r succeeded' % task
                                assert all(dep.id not in self.tasks or self.blocked[dep] for dep in task.dependents), \
                                       'not all dependents of %r blocked' % task

                                self.executable.remove(task)
                                self.executable_on[worker].discard(task)
                                self.pending.add(task)
                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug('%r executing on %r with locality %r',
                                                 task, worker, self.locality[worker].get(task, 0))
                                task.execute(self, self.workers[worker])
                            except CancelledError:
                                pass
                            except AssertionError:
                                raise
                            except Exception as exc:
                                task.mark_failed(exc)
                                self.task_done(task)
                        else:
                            self.workers_idle.add(worker)

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


    def select_task(self, worker):
        if not self.executable:
            return None

        # select a task for the worker
        worker_queue = self.executable_on[worker]
        for task in list(worker_queue):
            if task in self.pending or task in self.succeeded:
                # task executed by another worker
                worker_queue.remove(task)
            elif task in self.executable:
                return task
            elif self.blocked[task]:
                pass
            else:  # task not executable
                logger.error('%r not executable, blocked, pending nor executed', task)
                assert False, '%r not executable, blocked, pending nor executed' % task

        # no task available with locality > 0
        # find task which is allowed to execute on this worker
        for task in self.executable:
            if worker not in self.forbidden[task]:
                return task


    def set_executable(self, task):
        if task.id not in self.tasks:
            return

        assert not self.blocked[task], '%r isn\'t executable because it is blocked'
        assert all(dep.succeeded for dep in task.dependencies), 'not all dependencies of %r succeeded: %r' \
            % (task, [dep for dep in task.dependencies if not dep.succeeded])
        assert task not in self.succeeded, '%r already succeeded'

        if task in self.executable or task in self.pending or task.succeeded:
            return

        # calculate for each worker which tasks are forbidden or which have locality
        for worker in self.workers.keys():
            # don't bother with 'failed' workers
            if worker not in self.workers_failed:
                locality = self.locality[worker].get(task, 0)
                if locality >= 0:
                    # make sure the worker isn't 'stuck' in the idle set
                    if worker in self.workers_idle:
                        self.workers_idle.remove(worker)
                        for _ in range(self.concurrency):
                            self.workers_ready.append(worker)
                        self.condition.notify()

                    # the task has a preference for this worker
                    if locality > 0:
                        self.executable_on[worker].add(task)

        # check if there is a worker allowed to execute the task
        if len(self.forbidden[task]) == len(self.workers):
            raise Exception('%r cannot be executed on any available workers' % task)

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

        try:
            # nothing to do, scheduling was aborted
            if self._abort:
                return

            with self.lock:
                self.pending.discard(task)

                if task.failed:
                    self.task_failed(task)
                else:
                    assert task.succeeded, '%r not failed and not succeeded' % task
                    assert task not in self.succeeded, '%r completed while already in succeeded list' % task
                    logger.debug('%r was executed on %r', task, task.executed_on_last)
                    # add to executed and signal done
                    self.succeeded.add(task)
                    self.done(task)
                    # check for unblocking of dependents
                    for dependent in task.dependents:
                        blocked_by = self.blocked[dependent]
                        blocked_by.discard(task)
                        if not blocked_by and dependent:
                            if dependent in self.succeeded:
                                logger.debug('%r unblocked because %r was executed, but already succeeded', dependent, task)
                            else:
                                logger.debug('%r unblocked because %r was executed', dependent, task)
                                self.set_executable(dependent)

                self.workers_ready.append(task.executed_on_last)
                self.condition.notify()
        except Exception as exc:
            logger.exception('Unable to handle task completion of %r on %r',
                             task, task.executed_on_last)
            self.abort(exc)


    def task_failed(self, task):
        # in these cases we consider the task already re-scheduled
        if task in self.executable:
            logger.debug('%r failed with %s, but already marked as executable', task, type(root_exc(task.exception())).__name__)
            return
        elif task in self.pending:
            logger.debug('%r failed with %s, but already pending', task, type(root_exc(task.exception())))
            return

        assert task.failed, "Can't reschedule task %r which hasn't failed." % task

        exc = root_exc(task.exception())

        if isinstance(exc, DependenciesFailed):
            assert task not in self.succeeded, 'Dependencies of %r failed which already completed successfully' % task
            logger.info('%r failed on %s because %r failed, rescheduling',
                        task, task.executed_on_last,
                        ', '.join(worker + ': ' + ','.join(map(str, dependencies))
                                  for worker, dependencies in exc.failures.items()))

            for worker, dependencies in exc.failures.items():
                for task_id in dependencies:
                    try:
                        dependency = self.tasks[task_id]
                    except KeyError as e:
                        logger.error('Received DependenciesFailed for unknown task with id %r', task_id)
                        self.abort(e)
                    else:
                        # mark the worker as failed
                        executed_on_last = dependency.executed_on_last
                        if not worker or worker == executed_on_last:
                            if worker == executed_on_last:
                                logger.info('Marking %r as failed for dependency %s of %s',
                                            worker, dependency, task)
                                self.workers_failed.add(worker)
                                self.workers_idle.discard(worker)
                            dependency.mark_failed(FailedDependency(worker))
                            self.task_failed(dependency)
                        else:
                            # this should only occur with really really short tasks where the failure of a
                            # task noticed by task b is already obsolete because of the dependency was already
                            # restarted (because another task also issued DependenciesFailed)
                            logger.info('Received DependenciesFailed for task with id %r and worker %r '
                                        'but the task is last executed on %r',
                                         task_id, worker, executed_on_last)

        elif isinstance(exc, FailedDependency):
            self.succeeded.discard(task)
            worker = exc.worker_failed
            if worker:
                logger.info('%r marked as failed post-hoc, marking %r as failed', task, worker)
                self.workers_failed.add(worker)
                self.workers_idle.discard(worker)

        elif isinstance(exc, NotConnected):
            # mark the worker as failed
            logger.info('%r failed with NotConnected, marking %r as failed',
                        task, task.executed_on_last)
            self.workers_failed.add(task.executed_on_last)

        else:
            self.failures[task] = failures = self.failures[task] + 1
            if failures >= self.max_attempts:
                logger.warning('%r failed on %r after %r attempts ... aborting',
                               task, task.executed_on_last, len(task.executed_on))
                # signal done (failed) to allow bubbling up the error and abort
                self.done(task)
                self.abort(task.exception())
                return
            elif task.executed_on_last:
                logger.info('%r failed on %r with %s: %s, rescheduling',
                            task, task.executed_on_last, exc.__class__.__name__, exc)
            else:
                logger.info('%r failed before being executed with %s: %s, rescheduling',
                            task, exc.__class__.__name__, exc)

        # block its dependencies
        for dependent in task.dependents:
            # logger.debug('%r is blocked by %r because it failed', dependent, task)
            self.blocked[dependent].add(task)
            self.executable.discard(dependent)

        if len(self.workers_failed) == len(self.workers):
            self.abort(Exception('Unable to complete job, all workers failed'))

        if not self.blocked[task] and task not in self.executable and task not in self.pending:
            self.set_executable(task)

        assert self.blocked[task] or task in self.executable or task in self.pending
