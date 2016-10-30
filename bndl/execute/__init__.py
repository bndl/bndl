from bndl.util.conf import Int
from _collections_abc import Mapping


concurrency = Int(1, desc='the number of tasks which can be scheduled at a worker process at the same time')
max_attempts = Int(1, desc='the number of times a task is attempted before the job is cancelled')


class TaskCancelled(Exception):
    pass


class DependenciesFailed(Exception):
    def __init__(self, failures):
        assert isinstance(failures, Mapping), 'dependency failure must be a mapping of workers ' \
                                              'to a sequence of tasks which failed'
        self.failures = failures
