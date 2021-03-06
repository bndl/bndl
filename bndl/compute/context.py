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

from datetime import timedelta, datetime
import concurrent.futures
import copy
import logging
import time
import warnings

from bndl.compute.accumulate import Accumulator
from bndl.compute.broadcast import broadcast
from bndl.compute.driver import Driver
from bndl.compute.files import files
from bndl.compute.profile import MemoryProfiling, CpuProfiling
from bndl.compute.ranges import RangeDataset
from bndl.compute.scheduler import Scheduler
from bndl.compute.tasks import current_node
from bndl.compute.worker import start_worker
from bndl.net.aio import run_coroutine_threadsafe
from bndl.util import plugins
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from bndl.util.funcs import as_method
from bndl.util.funcs import noop
from bndl.util.lifecycle import Lifecycle
from bndl.util.threads import OnDemandThreadedExecutor


logger = logging.getLogger(__name__)



def _num_executors_connected():
    executor = current_node()
    return sum(1 for w in executor.peers.filter(node_type='executor') if w.is_connected)



class ComputeContext(Lifecycle):
    '''
    :class:`ComputeContext` is the main 'handle' into a cluster of BNDL workers and executors from
    the 'driver' node.

    It provides a means to create partitioned distributed data sets (which can be then transformed and
    combined), broadcast data, create accumulators to collect data into, etc.
    '''

    instances = set()


    @classmethod
    def get_or_create(cls):
        if len(cls.instances) > 0:
            return next(iter(cls.instances))
        else:
            return cls.create()


    @classmethod
    def create(cls):
        executor = OnDemandThreadedExecutor()
        load_plugins = executor.submit(plugins.load_plugins)

        driver = start_worker(Worker=Driver)
        stop = noop

        try:
            ctx = cls(driver)
            from bndl.util import dash
            dash.run(driver, ctx)
            load_plugins.result()

            def stop():
                dash.stop()
                driver.stop_async().result(5)

            def maybe_stop(obj):
                if obj is ctx and ctx.stopped:
                    stop()

            ctx.add_listener(maybe_stop)

            return ctx
        except Exception:
            stop()
            with catch():
                ctx.stop()
            raise


    def __init__(self, node, config=None):
        super().__init__()
        self._node = node
        self.conf = copy.copy(config) if config else Config()
        self.jobs = []
        self.signal_start()
        self.instances.add(self)
        self.stable_since = None

        self.scheduler = Scheduler()
        self.scheduler.start()

        def peer_listener(event, peer):
            if peer.node_type == 'executor':
                if event == 'added' :
                    self.scheduler.add_executor(peer)
                elif event == 'removed':
                    self.scheduler.remove_executor(peer)
        self._node.peers.listeners.add(peer_listener)
        for peer in self._node.peers.filter(node_type='executor'):
            self.scheduler.add_executor(peer)


    @property
    def default_pcount(self):
        '''
        The default number of partitions for data sets. This either the `bndl.compute.pcount`
        configuration value or the number of executors (`ctx.executor_count`) times the
        `bndl.compute.concurrency` configuration value (which defaults to 1).
        '''
        pcount = self.conf['bndl.compute.pcount']
        if pcount:
            return pcount
        if self.executor_count == 0:
            self.await_executors()
        return self.executor_count * self.conf['bndl.compute.concurrency']


    @property
    def node(self):
        if self._node is None:
            with catch(RuntimeError):
                self._node = current_node()
        return self._node


    def await_executors(self, executor_count=None, connect_timeout=5, stable_timeout=60):
        '''
        Waits for executors to be available. If not in connect_timeout a RuntimeError is raised. Once
        a executor is found, at most stable_timeout seconds will be waited for the cluster to settle.
        That is, until no new executors are discovered / the executor count is stable.

        Args:
            executor_count (float or None): The expected executor count. When connected to exactly executor_count
                executors, this method will return faster.
            connect_timeout (float): Maximum time in seconds waited until the first executor is discovered.
            stable_timeout (float): Maximum time in seconds waited until no more executors are discovered.
        '''
        if not isinstance(connect_timeout, timedelta):
            connect_timeout = timedelta(seconds=connect_timeout)
        if not isinstance(stable_timeout, timedelta):
            stable_timeout = timedelta(seconds=stable_timeout)

        # executors are awaited in a loop, idling step_sleep each time
        # step_sleep increases until at most 1 second
        step_sleep = .5

        # remember when we started the wait
        wait_started = datetime.now()
        # and set deadlines for first connect and stability
        connected_deadline = wait_started + connect_timeout
        stable_deadline = wait_started + stable_timeout
        # for stability we don't look back further than the stable timeout
        stable_max_lookback = self.stable_since or wait_started - stable_timeout

        def executor_count_consistent():
            '''Check if the executors all see each other'''
            logger.trace('Checking executor count across the cluster')
            expected = len(self.executors) - 1
            tasks = [(w, w.service('tasks').execute(_num_executors_connected)) for w in self.executors]
            deadline = time.time() + connect_timeout.total_seconds()
            consistent = True
            for executor, task in tasks:
                try:
                    timeout = deadline - time.time()
                    if timeout < 0:
                        return False
                    actual = task.result(timeout=timeout)
                    if actual != expected:
                        logger.debug('%r sees %s executors, not %s', executor.name, actual, expected)
                        consistent = False
                        run_coroutine_threadsafe(
                            executor.notify_discovery([(w.name, w.addresses) for w in self.executors]),
                            self.node.loop
                        ).result()
                        break
                except concurrent.futures.TimeoutError:
                    logger.debug("Couldn't get connected executor count from executor %r",
                                 executor.name, exc_info=True)
                    consistent = False
                    break
                except Exception:
                    logger.warning("Couldn't get connected executor count from executor %r",
                                   executor.name, exc_info=True)
                    consistent = False
                    break
            return consistent

        def is_stable():
            '''
            Check whether the cluster of executors is 'stable'.

            The following heuristics are applied (assuming the default timeout
            values):

            - The cluster is considered stable if no connects are made in the
              last 60 seconds.
            - If there is only one such recent connects, at least 5 seconds
              has passed.
            - If there are more recent connects, the cluster is considered
              stable if at least twice the maximum interval between the
              connects has passed or 1 second, whichever is longer.

            Returns (bool, bool) indicating whether connections were established recently
            and - if so - whether they have stabilized.
            '''
            assert len(self.executors) > 0
            now = datetime.now()
            recent_connects = sorted(executor.connected_on for executor in self.executors
                                     if not executor.connected_on or
                                        executor.connected_on > stable_max_lookback)

            # Stable if there weren't any (re-)connects in the last 60 seconds
            if not recent_connects:
                logger.trace('No recent connections made')
                return True

            # Unstable if there is only one connection and was made in the last 5 seconds
            elif len(recent_connects) == 1:
                if len(self.executors) == 1 and recent_connects[0] > now - connect_timeout:
                    logger.trace('One connection in the %s, wait', connect_timeout)
                    return False

            # Stable if there are multiple connections but at least twice the maximum gap between
            # connects as passed
            else:
                max_connect_gap = max(b - a for a, b in zip(recent_connects, recent_connects[1:]))
                stable_time = now - min(connect_timeout, 10 * max_connect_gap)
                if recent_connects[-1] > stable_time:
                    logger.trace('Wait at least %s', stable_time)
                    return False

            # Stable if each node sees the same amount of executors
            return executor_count_consistent()

        while True:
            if executor_count == len(self.executors):
                self.stable_since = datetime.now()
                return executor_count

            if len(self.executors) == 0:
                if datetime.now() > connected_deadline:
                    raise RuntimeError('no executors available')
            else:
                stable = is_stable()

                if stable:
                    self.stable_since = datetime.now()
                    return len(self.executors)
                elif datetime.now() > stable_deadline:
                    warnings.warn('executor count not stable after %s' % stable_timeout)
                    return len(self.executors)

            time.sleep(step_sleep)
            step_sleep = min(connect_timeout.total_seconds(), step_sleep * 1.5)


    @property
    def worker_count(self):
        '''The number of workers connected with'''
        wc = len(self.workers)
        if wc:
            return wc
        else:
            if not self.executors:
                self.await_executors()
            return len(self.workers)


    @property
    def workers(self):
        '''All peers of the local node with ``node_type`` `worker`.'''
        return self.node.peers.filter(node_type='worker')


    @property
    def executor_count(self):
        '''The number of executors connected with'''
        ec = len(self.executors)
        if ec:
            return ec
        else:
            return self.await_executors()

    @property
    def executors(self):
        '''All peers of the local node with ``node_type`` `executor`.'''
        return self.node.peers.filter(node_type='executor')


    cpu_profiling = property(as_method(CpuProfiling))
    memory_profiling = property(as_method(MemoryProfiling))


    def collection(self, collection, pcount=None, psize=None):
        '''
        Create a data set from a python iterable (e.g. a list, dict or iterator).

        Note that iterators are consumed immediately since data sets need to be deterministic in
        their output.

        By default pcount (or `ctx.default_pcount`) partitions are created. pcount must *not* be
        set when psize is set.

        Args:

            collection (iterable): The iterable to partition and distribute.
            pcount (int or None): The number of partition to build.
            psize (int or None): The maximum number of records per partition.

        Example::

            >>> ctx.collection(list('abcd')).nlargest(2)
            ['d', 'c']
        '''
        if isinstance(collection, range):
            return self.range(collection.start, collection.stop, collection.step, pcount=pcount)
        else:
            from bndl.compute.collections import DistributedCollection
            return DistributedCollection(self, collection, pcount, psize)


    def range(self, start, stop=None, step=1, pcount=None):
        '''
        Create a distributed partitioned data set of a range of numbers.

        Args:
            start (int): The start or stop value (if no stop value is given).
            stop (int): The stop value or None if start is the stop value.
            step (int): The step between each value in the range.
            pcount (int): The number of partitions to partition the range into.

        Examples::

            >>> ctx.range(10).collect()
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            >>> ctx.range(10).glom().collect()
            [range(0, 2), range(2, 5), range(5, 7), range(7, 10)]
            >>> ctx.range(5, 20).glom().map(len).collect()
            [3, 4, 4, 4]
            >> ctx.range(5, 10).stats()
            <Stats count=5, mean=7.0, min=5.0, max=9.0, var=2.0, stdev=1.4142135623730951, skew=0.0, kurt=-1.3>

        '''
        return RangeDataset(self, start, stop, step, pcount)


    def accumulator(self, initial):
        '''
        Create an :class:`Accumulator <bndl.compute.accumulate.Accumulator>` with an initial value.

        Args:
            initial: The initial value of the accumulator.

        Example::

            >>> from bndl.compute import ctx
            >>> accum = ctx.accumulator(0)
            >>> def mapper(i):
            ...     global accum
            ...     accum += i
            ...     return i
            ...
            >>> ctx.range(10).sum()
            45
            >>> accum.value
            0
            >>> ctx.range(10).map(mapper).sum()
            45
            >>> accum.value
            45
            >>> ctx.range(10).map(mapper).sum()
            45
            >>> accum.value
            90
        '''
        accumulator = Accumulator(self, self.node.name, initial)
        self.node.service('accumulate').register(accumulator)
        return accumulator


    broadcast = broadcast
    files = files


    def stop(self):
        self.scheduler.stop()
        # TODO self.scheduler.join()
        self.signal_stop()
        try:
            self.instances.remove(self)
        except KeyError:
            pass


    def execute(self, job, executors=None, order_results=True, concurrency=None, attempts=None):
        '''
        Execute a :class:`Job <bndl.compute.job.Job>` on executors and get the results of each
        :class:`Task <bndl.compute.job.Task>` as it is executed.

        Args:
            job (bndl.compute.job.Job): The job to execute.
            executors (sequence): A sequence of :class:`RMIPeerNodes <bndl.net.rmi.RMIPeerNode>`
                peer nodes to execute the job on.
            order_results (bool): Whether the results of the task are to be yielded in order or not
                (defaults to True).
            concurrency (int >= 1): The number of tasks to execute concurrently on each executor.
                Defaults to the ``bndl.compute.concurrency`` configuration parameter.
            attempts (int >= 1): The maximum number of attempts per task (not counting executor
                failure as induced from a task failing with NotConnected or a task marked as failed
                through :class:`bndl.compute.scheduler.DependenciesFailed`). Defaults to the
                ``bndl.compute.attempts`` configuration parameter.
        '''
        assert self.running, 'context is not running'
#         assert concurrency is None or concurrency >= 1
#         assert attempts is None or attempts >= 1
        assert concurrency is None or concurrency == 1
        assert attempts is None or attempts == 1

        if not job.tasks:
            return


        if executors is None:
            executors = self.executors
        if executors is None:
            self.await_executors()
            executors = self.executors

        self.jobs.append(job)
        job.signal_start()

        try:
            yield from self.scheduler.execute(job.tasks, order_results)
        finally:
            job.signal_stop()


    def __getstate__(self):
        state = super().__getstate__()
        state.pop('_node', None)
        state.pop('jobs', None)
        state.pop('cancelled', None)
        state.pop('name', None)
        state.pop('started_on', None)
        state.pop('stopped_on', None)
        state.pop('scheduler', None)
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self._node = None
