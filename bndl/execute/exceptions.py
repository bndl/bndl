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
