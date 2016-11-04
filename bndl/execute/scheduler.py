from collections  import defaultdict, deque, OrderedDict
from concurrent.futures import CancelledError
from functools import partial
from threading import Condition, RLock
import logging

from bndl.execute import DependenciesFailed
from bndl.net.connection import NotConnected
from sortedcontainers import SortedSet


logger = logging.getLogger(__name__)


class Scheduler(object):
    '''
    TODO
    
    Worker assignment takes into account
    - concurrency (how many tasks must a worker execute concurrently)
    - and worker locality (0 is indifferent, -1 is forbidden, 1+ increasing locality)
      as locality 0 is likely to be common, this is assumed throughout the scheduler
      to reduce the memory cost for scheduling
    '''

    def __init__(self, ctx, tasks, done, workers=None, concurrency=None, attempts=None):
        '''
        Execute tasks in the given context and invoke done(task) when a task completes.
        
        :param ctx: ComputeContext
        :param tasks: iterable[task]
        :param done: callable(task)
            Invoked when a task completes. Must be thread safe. May be called multiple times
            if a task is reran (e.g. in case a worker fails). done(None) is called to signal
            completion of the last task.
        :param: workers: sequence[Peer] or None
            Optional sequence of workers to execute on. ctx.workers is used if not provided.
        :param: concurrency: int or None
            @see: bndl.execute.concurrency
        :param: attempts: int or None
            @see: bndl.execute.attempts
        '''
        self.tasks = OrderedDict((task.id, task) for task
                                 in sorted(tasks, key=lambda t: t.priority))
        if len(self.tasks) == 0:
            raise ValueError('Tasks must provide at least one task to execute')
        if len(self.tasks) < len(tasks):
            raise ValueError('Tasks must have a unique task ID')

        self.done = done
        self.workers = {worker.name:worker for worker in (workers or ctx.workers)}

        self.concurrency = concurrency or ctx.conf['bndl.execute.concurrency']
        # failed tasks are retried on error, but they are executed at most attempts
        self.attempts = attempts or ctx.conf['bndl.execute.attempts']

        # task completion is (may be) executed on another thread, this lock serializes access
        # on the containers below and workers_idle
        self.lock = RLock()
        # a condition is used to signal that a worker is available or the scheduler is aborted
        self.condition = Condition(self.lock)


    def run(self):
        logger.info('executing job with %r tasks', len(self.tasks))

        self._abort = False
        self._exc = False

        # containers for states a task can be in
        self.executable = SortedSet(key=lambda task: task.priority)  # sorted executable tasks (sorted by task.id by default)
        self.blocked = defaultdict(set)  # blocked tasks task -> dependencies executable or executing

        self.locality = {worker:{} for worker in self.workers.keys()}  # worker_name -> task -> locality > 0
        self.forbidden = defaultdict(set)  # task -> set[worker]
        # worker -> SortedList[task] in descending locality order
        self.executable_on = {worker:SortedSet(key=lambda task, worker=worker:-self.locality[worker].get(task, 0))
                              for worker in self.workers.keys()}

        self.executing = set()  # mapping of task -> worker for tasks which are currently executing
        self.executed = set()  # tasks which have been executed

        # keep a FIFO queue of workers ready
        # and a list of idle workers (ready, but no more tasks to execute)
        self.workers_ready = deque()
        self.workers_idle = set(self.workers.keys())
        self.workers_failed = set()

        # perform scheduling under lock
        try:
            with self.lock:
                logger.debug('calculating which tasks are executable, which are blocked and if there is locality')

                # create list of executable tasks and set of blocked tasks
                for task in self.tasks.values():
                    # TODO don't ask on a per worker basis for locality, but ask with list of workers
                    for worker, locality in task.locality(self.workers.values()) or ():
                        if locality < 0:
                            self.forbidden[task].add(worker.name)
                        elif locality > 0:
                            self.locality[worker.name][task] = locality

                    if task.done:
                        self.executed.add(task)
                        self.done(task)
                    elif task.dependencies:
                        not_done = [dep for dep in task.dependencies if not dep.done]
                        if not_done:
                            self.blocked[task].update(not_done)
                        else:
                            self.set_executable(task)
                    else:
                        self.set_executable(task)

                if not self.executable:
                    raise Exception('No tasks executable (all tasks have dependencies)')
                if not self.workers_ready:
                    raise Exception('No workers available (all workers are forbidden by all tasks)')

                logger.debug('starting %s tasks (%s tasks blocked) on %s workers',
                             len(self.executable), len(self.blocked), len(self.workers_ready))

                while True:
                    # wait for a worker to become available (signals task completion
                    self.condition.wait_for(lambda: self.workers_ready or self._abort)

                    if self._abort:
                        # the abort flag can be set to True to break the loop (in case of emergency)
                        for task in self.tasks.values():
                            if task in self.executing:
                                task.cancel()
                        break

                    worker = self.workers_ready.popleft()

                    if worker in self.workers_failed:
                        # the worker is 'ready' (a task was 'completed'), but with an error
                        # or the worker was marked as failed because another task depended on an output
                        # on this worker and the dependency failed
                        continue
                    elif not (self.executable or self.executing):
                        # continue work while there are executable tasks or tasks executing or break
                        break
                    else:
                        task = self.select_task(worker)
                        if task:
                            # execute a task on the given worker and add the task_done callback
                            # the task is added to the executing set
                            try:
                                self.executable.remove(task)
                                self.executable_on[worker].discard(task)
                                self.executing.add(task)
                                future = task.execute(self.workers[worker])
                                future.add_done_callback(partial(self.task_done, task, worker))
                            except CancelledError:
                                pass
                        else:
                            self.workers_idle.add(worker)

                logger.info('completed %s tasks', len(self.executed))

        except Exception as exc:
            self._exc = exc

        if self._exc:
            self.done(self._exc)
        else:
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
            if task in self.executing or task in self.executed:
                # task executed by another worker
                worker_queue.remove(task)
            elif task in self.executable:
                return task
            else:  # task not executable
                assert False, 'task %s not in executable, executing nor executed' % task

        # no task available with locality > 0
        # find task which is allowed to execute on this worker
        for task in self.executable:
            if worker not in self.forbidden[task]:
                return task


    def set_executable(self, task):
        if task in self.executable or task in self.executing or task in self.executed:
            return

        # in case the task was already executed clear this state
        self.executed.discard(task)

        # calculate for each worker which tasks are forbidden or which have locality
        # TODO don't ask on a per worker basis for locality, but ask with list of workers
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
            raise Exception('Task %r cannot be executed on any available workers' % task)

        # add the task to the executable queue
        self.executable.add(task)


    def task_done(self, task, worker, future):
        '''
        When a task completes, delete it from executing, add it to done
        and set dependent tasks as executable if this task was the last dependency.
        Reschedule failed tasks or abort scheduling if failed to often.
        '''
        try:
            # nothing to do, scheduling was aborted
            if self._abort:
                return

            with self.lock:
                # remove from executing
                self.executing.discard(task)

                if task.failed:
                    self.task_failed(task)
                else:
                    # add to executed
                    self.executed.add(task)
                    # signal done
                    self.done(task)
                    # check for unblocking of dependents
                    for dependent in task.dependents:
                        blocked_by = self.blocked[dependent]
                        blocked_by.discard(task)
                        if not blocked_by:
                            self.set_executable(dependent)

                self.workers_ready.append(worker)
                self.condition.notify()
        except Exception:
            logger.exception('Unable to handle task completion')
            self.abort()


    def task_failed(self, task):
        # in these cases we consider the task already re-scheduled
        if task in self.executable or task in self.executing or self.blocked[task]:
            return

        exc = task.exception()

        self.executed.discard(task)

        # fail its dependencies
        for dependent in task.dependents:
            self.blocked[dependent].add(task)

        if len(task.executed_on) >= self.attempts:
            logger.warning('Task %r failed on %s after %s attempts ... aborting',
                           task, task.executed_on_last, len(task.executed_on))
            # signal done (failed) to allow bubbling up the error and abort
            self.done(task)
            self.abort()
            return

        logger.info('Task %r failed on %s with %r, rescheduling',
                    task, task.executed_on_last, type(exc))

        if isinstance(exc, DependenciesFailed):
            for worker, dependencies in exc.failures.items():
                for task_id in dependencies:
                    try:
                        dependency = self.tasks[task_id]
                    except KeyError:
                        logger.error('Receive DependenciesFailed for unknown task with id %s' % task_id)
                        self.abort()
                    else:
                        # mark the worker as failed
                        executed_on_last = dependency.executed_on_last
                        if worker == executed_on_last:
                            logger.info('marking %s as failed for dependency %s of %s',
                                        worker, dependency, task)
                            self.workers_failed.add(executed_on_last)
                            self.workers_idle.discard(executed_on_last)
                            # mark task as failed and reschedule
                            dependency.set_exception(Exception('Marked as failed by task %r' % task))
                            self.task_failed(dependency)
                        else:
                            # this should only occur with really really short tasks where the failure of a
                            # task noticed by task b is already obsolete because of the dependency was already
                            # restarted (because another task also issued DependenciesFailed)
                            logger.info('Receive DependenciesFailed for task with id %s and worker name %s '
                                        'but the task is last executed on %s',
                                         task_id, worker, executed_on_last)

        elif isinstance(exc, NotConnected):
            # mark the worker as failed
            logger.info('marking %s as failed because %s failed with NotConnected',
                        task.executed_on_last.name, task)
            self.workers_failed.add(task.executed_on_last)

        if len(self.workers_failed) == len(self.workers):
            try:
                raise Exception('Unable to complete job, all workers failed')
            except Exception as exc:
                self.abort(exc)

        if not self.blocked[task] and task not in self.executable and task not in self.executing:
            self.set_executable(task)
