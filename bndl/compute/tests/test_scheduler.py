from collections import deque
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import product, chain
from unittest import TestCase
import signal
import time

from toolz.itertoolz import interleave

from bndl.compute.job import Task
from bndl.compute.scheduler import Scheduler, DependenciesFailed
from bndl.net.connection import NotConnected
from bndl.util.conf import Config
from bndl.util.threads import dump_threads
import numpy as np


signal.signal(signal.SIGUSR1, dump_threads)


rng = np.random.RandomState(2)
NOTSET = object()


class DummyContext(object):
    def __init__(self):
        self.conf = Config()


class DummyTask(Task):
    def __init__(self, *args, sleep=(.01, .01), result=NOTSET, **kwargs):
        if 'max_attempts' not in kwargs and isinstance(result, deque):
            kwargs['max_attempts'] = len(result)
        super().__init__(*args, **kwargs)
        self.sleep = sleep
        self._result = result

    def execute(self, scheduler, executor):
        super().set_executing(executor)
        executor.execute(self)

    def run(self):
        try:
            if not self.sleep[0] or rng.uniform() > self.sleep[0]:
                time.sleep(rng.uniform() * self.sleep[1])
            res = self._result
            if isinstance(res, deque):
                res = res.popleft()
            if isinstance(res, Exception):
                self.mark_failed(res)
            elif res is NOTSET:
                self.mark_done(self.id)
            else:
                self.mark_done(res)
        except Exception as exc:
            self.mark_failed(exc)



class BarrierTask(DummyTask):
    def __init__(self, *args, sleep=(0, 0), **kwargs):
        super().__init__(*args, sleep=sleep, **kwargs)



class DummyExecutor(object):
    def __init__(self, name):
        self.name = name
        self.threadpool = ThreadPoolExecutor(1)
        self.is_connected = True

    def execute(self, task):
        self.threadpool.submit(task.run)

    def __repr__(self):
        return '<Executor %s>' % self.name



def link(*groups):
    for tasks_a, tasks_b in zip(groups, groups[1:]):
        for a, b in product(tasks_a, tasks_b):
            a.dependents.add(b)
            b.dependencies.add(a)



class SchedulerTest(TestCase):
    executor_count = 3
    group_task_count = 5

    def setUp(self):
        self.ctx = DummyContext()
        self.scheduler = Scheduler()
        self.scheduler.start()
        self.executors = [DummyExecutor(str(i)) for i in range(self.executor_count)]
        for e in self.executors:
            self.scheduler.add_executor(e)


    def tearDown(self):
        time.sleep(.01)

        try:
            sched = self.scheduler
            self.assertEqual(len(sched.executors), len(self.executors))
            self.assertEqual(sorted(e.name for e in self.executors), sorted(sched.executors_ready))
            self.assertEqual(len(sched.executable), 0)
            self.assertEqual(len(sched.tasks), 0)
            self.assertEqual(len(sched.locality), 0)
            self.assertEqual(sum(len(tasks) for tasks in sched.executable_on.values()), 0)
            self.assertEqual(sum(len(tasks) for tasks in sched.executing_on.values()), 0)
            self.assertEqual(sum(len(tasks) for tasks in sched.forbidden_on.values()), 0)

            sched.stop()
            # TODO self.scheduler.join()
            self.assertEqual(sched.running, False)
        except:
            sched.debug()
            raise

    def assert_attempts_equal(self, tasks, attempts):
        for task in tasks:
            self.assertEqual(task.attempts, attempts, task)


    def _execute(self, tasks):
        return [(t, t.result) for t in self.scheduler.execute(tasks)]


    def _test_sunny(self, *tasks):
        link(*tasks)
        tasks = list(chain.from_iterable(tasks))
        res = self._execute(tasks)
        self.assert_attempts_equal(tasks, 1)
        self.assertEqual([t.id for t in tasks], [t.id for t, _ in res])


    def _create_tasks(self, group, n=None, cls=DummyTask):
        return [cls(self.ctx, (group, i + 1)) for i in range(n or self.group_task_count)]


    def _create_groups(self, n, barriers=True):
        tasks = [self._create_tasks(i + 1) for i in range(n)]
        if barriers:
            barriers = [[BarrierTask(self.ctx, (i + 2, 0))] for i in range(n - 1)]
            tasks = list(interleave((tasks, barriers)))
        link(*tasks)
        return tasks


#     def test_sunny(self):
#         for barrier in (False, True):
#             for groups in (1, 2, 3, 5, 9):
#                 self._test_sunny(*self._create_groups(groups, barrier))


#     def test_permanent_failure(self):
#         a, ab, b, bc, c = self._create_groups(3)
#         tasks = a + ab + b + bc + c
#
#         x = len(a) + len(ab) + len(b) - 2
#         tasks[x].max_attempts = 2
#         tasks[x]._result = deque([Exception('failure 1'),
#                                   Exception('failure 2'),
#                                   Exception('failure 3')])
#
#         with self.assertRaisesRegex(Exception, 'failure 2'):
#             list(self.scheduler.execute(tasks))
#
#         self.assert_attempts_equal(tasks[:x], 1)
#         self.assert_attempts_equal(tasks[x:x + 1], 2)
#         self.assert_attempts_equal(bc, 0)
#         self.assert_attempts_equal(c, 0)


    def test_transient_task_failure(self):
        a, ab, b, bc, c = self._create_groups(3)
        tasks = a + ab + b + bc + c

        x = len(b) - 2
        b[x].max_attempts = 2
        b[x]._result = deque([Exception('failed'), (2, x + 1)])

        res = self._execute(tasks)

        x += len(a) + len(ab)
        self.assert_attempts_equal(tasks[:x], 1)
        self.assert_attempts_equal(tasks[x:x + 1], 2)
        self.assert_attempts_equal(tasks[x + 1:], 1)

        self.assertEqual([t.id for t in tasks], [t.id for t, _ in res])
        self.assertEqual([t.id for t in tasks], [r for _, r in res])


    def test_executor_notconnected(self):
        a, ab, b, bc, c = self._create_groups(3)
        tasks = a + ab + b + bc + c

        x = len(b) - 2
        b[x]._result = deque([NotConnected(), (2, x + 1)])

        res = self._execute(tasks)

        x += len(a) + len(ab)
        self.assert_attempts_equal(tasks[:x], 1)
        self.assert_attempts_equal(tasks[x:x + 1], 2)
        self.assert_attempts_equal(tasks[x + 1:], 1)

        self.assertEqual([t.id for t in tasks], [t.id for t, _ in res])
        self.assertEqual([t.id for t in tasks], [r for _, r in res])


    def test_executor_depsfailed(self):
        a, ab, b, bc, c = self._create_groups(3)
        tasks = a + ab + b + bc + c

        failures_b = {None: {None: []}}
        failures_c = {None: {None: []}}
        for group, failures in ((a, failures_b), (b, failures_c)):
            for t in rng.choice(group, len(group) // 2, replace=False):
                failures[None][None].append(t.id)

        x = len(b) - 2
        b[x]._result = deque([DependenciesFailed(failures_b)] + [(2, x + 1)] * 2)

        x = len(c) - 2
        c[x]._result = deque([DependenciesFailed(failures_c)] + [(3, x + 1)] * 2)

        res = self._execute(tasks)

        self.assertEqual(tasks, [t for t, _ in res])
        self.assertEqual([t.id for t in tasks], [t.id for t, _ in res])
        self.assertEqual([t.id for t in tasks], [r for _, r in res])


    def test_already_succeeded(self):
        a, ab, b, bc, c = self._create_groups(3)
        tasks = a + ab + b + bc + c
        link(a, ab, b, bc, c)

        for g in (a, b):
            for t in g:
                t.mark_done(None)

        res = self._execute(tasks)
        self.assert_attempts_equal(a, 0)
        self.assert_attempts_equal(b, 0)
        self.assert_attempts_equal(c, 1)
        self.assertEqual([t.id for t in tasks], [t.id for t, _ in res])


    def test_cancel(self):
        a, ab, b, bc, c = self._create_groups(3)
        tasks = a + ab + b + bc + c
        link(a, ab, b, bc, c)

        for t in a:
            t.sleep = (0, 2)

        def kill():
            next(gen)
            gen.throw(KeyboardInterrupt)

        gen = self.scheduler.execute(tasks)
        threadpool = ThreadPoolExecutor()
        res = threadpool.submit(kill)
        time.sleep(.01)
        a[0].mark_done(True)
        with self.assertRaises(KeyboardInterrupt):
            res.result(1)
