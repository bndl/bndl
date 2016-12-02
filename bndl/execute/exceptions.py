class TaskCancelled(Exception):
    '''
    Exception raised in a worker when a task is to be cancelled (preempted) by the driver.

    This exception is raised through use of
    `PyThreadState_SetAsyncExc <https://docs.python.org/3.5/c-api/init.html#c.PyThreadState_SetAsyncExc>`_.
    '''


class DependenciesFailed(Exception):
    '''
    Indicate that a task failed due to dependencies not being 'available'. This will cause the
    dependencies to be re-executed and the task which raises DependenciesFailed will be scheduled
    to execute once the dependencies complete.

    The failures attribute is a mapping from worker names (strings) to a sequence of
    task_ids which have failed.
    '''
    def __init__(self, failures):
        self.failures = failures
