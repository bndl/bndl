'''
The ``bndl.execute`` module provides a means to execute :class:`Jobs <bndl.execute.job.Job>` on a
cluster of workers.

:class:`Jobs <bndl.execute.job.Job>` consists of :class:`Tasks <bndl.execute.job.Task>` which are
interconnected in a directed acyclic graph (traversable in both directions). ``bndl.execute``
provides a :class:`Scheduler <bndl.execute.scheduler.Scheduler>` which executes jobs and ensure
that tasks are executed in dependency order, tasks are executed with locality (if any) to a worker
and tasks are restarted (if configured), also when a (possibly indirect) dependent task marks a
dependency as failed.
'''

from bndl.util.conf import Int

from .exceptions import *

concurrency = Int(1, desc='the number of tasks which can be scheduled at a worker process at the same time')
attempts = Int(1, desc='the number of times a task is attempted before the job is cancelled')
