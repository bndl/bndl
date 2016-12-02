'''
The ``bndl.execute`` module provides a means to execute :class:`Jobs <bndl.execute.job.Job>` (consisting of
:class:`Tasks <bndl.execute.job.Task>`) on a cluster of workers.
'''

from bndl.util.conf import Int

from .exceptions import *

concurrency = Int(1, desc='the number of tasks which can be scheduled at a worker process at the same time')
attempts = Int(1, desc='the number of times a task is attempted before the job is cancelled')
