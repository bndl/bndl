from collections  import defaultdict, deque, OrderedDict
from concurrent.futures import CancelledError
from functools import partial
from threading import Condition, RLock
import logging

from bndl.execute import DependenciesFailed
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
        if len(self.tasks) ==0:
            raise ValueError('Tasks must provide at least one task to execute')
        if len(self.tasks) < len(tasks):
            raise ValueError('Tasks must have a unique task ID')

        self.done = done
        self.workers = workers or ctx.workers

        self.concurrency = concurrency or ctx.conf['bndl.execute.concurrency']
        # failed tasks are retried on error, but they are executed at most attempts
        self.attempts = attempts or ctx.conf['bndl.execute.attempts']

        # task completion is (may be) executed on another thread, this lock serializes access
        # on the containers below and workers_idle
        self.lock = RLock()
        # a condition is used to signal that a worker is available or the scheduler is aborted
        self.condition = Condition(self.lock)


    def run(self):
        logger.info('running job with %r tasks', len(self.tasks))
        
        self._abort = False

        # containers for states a task can be in
        self.runnable = SortedSet(key=lambda task: task.priority)  # sorted runnable tasks (sorted by task.id by default)
        self.blocked = defaultdict(set)  # blocked tasks task -> dependencies runnable or running

        self.locality = {worker:{} for worker in self.workers}  # worker -> task -> locality > 0
        self.forbidden = defaultdict(set)  # task -> set[worker]
        # worker -> SortedList[task] in descending locality order
        self.runnable_on = {worker:SortedSet(key=lambda task, worker=worker:self.locality[worker].get(task, 0))
                            for worker in self.workers}

        self.running = set()  # mapping of task -> worker for tasks which are currently running
        self.running_on = defaultdict(set)  # mapping of worker -> set[task] for tasks which are currently running

        self.executed = set()  # tasks which have been executed
        self.executed_on = defaultdict(set)  # mapping of worker -> set[task] for tasks which have been executed

        # keep a FIFO queue of workers ready
        # and a list of idle workers (ready, but no more tasks to execute)
        self.workers_ready = deque()
        self.workers_idle = set(self.workers)
        self.workers_failed = set()

        # perform scheduling under lock
        with self.lock:
            logger.debug('calculating which tasks are runnable, which are blocked and if there is locality')
            
            # create list of runnable tasks and set of blocked tasks
            for task in self.tasks.values():
                if task.dependencies:
                    self.blocked[task].update(task.dependencies)
                else:
                    self.set_runnable(task)

                # TODO don't ask on a per worker basis for locality, but ask with list of workers
                for worker, locality in task.locality(self.workers) or ():
                    if locality < 0:
                        self.forbidden[task].add(worker)
                    elif locality > 0:
                        self.locality[worker][task] = locality

            if not self.runnable:
                raise Exception('No tasks runnable (all tasks have dependencies)')
            if not self.workers_ready:
                raise Exception('No workers available (all workers are forbidden by all tasks)')
            
            logger.debug('starting %s tasks (%s tasks blocked) on %s workers',
                         len(self.runnable), len(self.blocked), len(self.workers_ready))
            
            while True:
                # wait for a worker to become available (signals task completion
                self.condition.wait_for(lambda: self.workers_ready or self._abort)

                if self._abort:
                    # the abort flag can be set to True to break the loop (in case of emergency)
                    for task in self.running:
                        task.cancel()
                    break
                
                worker = self.workers_ready.popleft()

                if worker in self.workers_failed:
                    # the worker is 'ready' (a task was 'completed'), but with an error
                    # or the worker was marked as failed because another task depended on an output
                    # on this worker and the dependency failed
                    continue
                elif not (self.runnable or self.running):
                    # continue work while there are runnable tasks or tasks running or break
                    break
                else:
                    task = self.select_task(worker)
                    if task:
                        # execute a task on the given worker and add the task_done callback
                        # the task is added to the running set
                        try:
                            self.runnable.remove(task)
                            self.runnable_on[worker].discard(task)
                            self.running.add(task)
                            self.running_on[worker].add(task)
                            future = task.execute(worker)
                            future.add_done_callback(partial(self.task_done, task, worker))
                        except CancelledError:
                            pass
                    else:
                        self.workers_idle.add(worker)
            
            logger.info('completed %s tasks', len(self.executed))

            self.done(None)


    def abort(self):
        with self.lock:
            self._abort = True
            self.condition.notify()



    def select_task(self, worker):
        if not self.runnable:
            return None

        # select a task for the worker
        worker_queue = self.runnable_on[worker]
        for task in list(worker_queue):
            if task in self.running or task in self.executed:
                # task executed by another worker
                worker_queue.remove(task)
            elif task in self.runnable:
                return task
            else:  # task not runnable
                if not self.blocked[task] or (task.done and not task.failed):
                    self.dump_state()

        # no task available with locality > 0
        # find task which is allowed to run on this worker
        for task in self.runnable:
            if worker not in self.forbidden[task]:
                return task


    def set_runnable(self, task):
        if task in self.runnable or task in self.running or task in self.executed:
            return

        if task.executed_on:  # in case the task was already executed clear this state
            self.executed.discard(task)
            self.executed_on[task.executed_on[-1]].discard(task)

        # calculate for each worker which tasks are forbidden or which have locality
        # TODO don't ask on a per worker basis for locality, but ask with list of workers
        for worker in self.workers:
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
                        self.runnable_on[worker].add(task)

        # check if there is a worker allowed to run the task
        if len(self.forbidden[task]) == len(self.workers):
            raise Exception('Task %r cannot be run on any available workers' % task)

        # add the task to the runnable queue
        self.runnable.add(task)


    def task_done(self, task, worker, future):
        '''
        When a task completes, delete it from running, add it to done
        and set dependent tasks as runnable if this task was the last dependency.
        Reschedule failed tasks or abort scheduling if failed to often.
        '''
        # nothing to do, scheduling was aborted
        if self._abort:
            task.cancel()
            return

        with self.lock:
            # remove from running
            self.running.discard(task)
            self.running_on[worker].discard(task)

            if task.failed:
                self.task_failed(task)
            else:
                # add to executed
                self.executed.add(task)
                self.executed_on[worker].add(task)
                # signal done
                self.done(task)
                # check for unblocking of dependents
                for dependent in task.dependents:
                    blocked_by = self.blocked[dependent]
                    blocked_by.discard(task)
                    if not blocked_by:
                        self.set_runnable(dependent)

            self.workers_ready.append(worker)
            self.condition.notify()


    def task_failed(self, task):
        self.executed.discard(task)
        self.executed_on[task.executed_on[-1]].discard(task)

        for dependent in task.dependents:
            self.blocked[dependent].add(task)
            self.runnable.discard(dependent)
            if dependent.running:
                dependent.cancel()

        if task in self.runnable or task in self.running or self.blocked[task]:
            return
        elif len(task.executed_on) >= self.attempts:
            # signal done (failed) to allow bubbling up the error and abort
            self.done(task)
            self.abort()
            return

        exc = task.exception()
        if isinstance(exc, DependenciesFailed):
            for dependency_id in exc.task_ids:
                dependency = self.tasks[dependency_id]
                # mark the worker as failed
                worker = dependency.executed_on[-1]
                self.workers_failed.add(worker)
                self.workers_idle.discard(worker)
                # mark task as failed and reschedule
                dependency.mark_failed(Exception('Marked as failed by task %r' % task))
                self.task_failed(dependency)
        else:
            # mark the worker as failed
            self.workers_failed.add(task.executed_on[-1])

        if not self.blocked[task] and task not in self.runnable and task not in self.running:
            self.set_runnable(task)
