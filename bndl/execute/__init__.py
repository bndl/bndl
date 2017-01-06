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
