from bndl.util.conf import Int


concurrency = Int(1, desc='the number of tasks which can be scheduled at a worker process at the same time')
attempts = Int(1, desc='the number of times a task is attempted before the job is cancelled')


class TaskCancelled(Exception):
    pass


class DependenciesFailed(Exception):
    def __init__(self, failures):
        '''
        Indicate that a task failed due to dependencies not being 'available'. This will cause the dependencies
        to be re-executed and the task which raises DependenciesFailed will be scheduled to run once the
        dependencies complete.

        :param failures: Mapping[worker: Sequence[task_id]]
        '''
        self.failures = failures
