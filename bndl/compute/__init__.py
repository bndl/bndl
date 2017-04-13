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
The ``bndl.compute`` module provides a means to construct partitioned
:class:`Datasets <bndl.compute.dataset.Dataset>` from a variety of sources and provides operators
which transform and combine these data sets.
'''

import os

from bndl.compute.context import ComputeContext
from bndl.compute.run import create_ctx
from bndl.util import conf
from bndl.util.objects import LazyObject
import resource


numactl = conf.Bool(default=True, desc='Pin processe (workers) to specific NUMA zones with numactl.')
pincore = conf.Bool(default=False, desc='Pin processes (workers) to specific cores with taskset.')
jemalloc = conf.Bool(default=True, desc='Use jemalloc if available.')

executor_count = conf.Int(desc='The number of executors to start when no seeds are '
                               'given when using bndl-compute-shell or bndl-compute-worker')

pcount = conf.Int(desc='The default number of partitions. Then number of connected executors is '
                       'used if not set.')

concurrency = conf.Int(1, desc='the number of tasks which can be scheduled at an executor process '
                               'at the same time')

attempts = conf.Int(1, desc='The number of times a task is attempted before the job is cancelled')


# Make sure numpy, scipy etc don't ask OMP to create as much threads as there are cores as BNDL is
# already parallelizing work.
if 'OMP_NUM_THREADS' not in os.environ:
    os.environ['OMP_NUM_THREADS'] = '2'

# Set the soft limit for the maximum number of open files to the hard limit.
for r in (resource.RLIMIT_NOFILE,):
    low, high = resource.getrlimit(r)
    if low < high or high == resource.RLIM_INFINITY:
        resource.setrlimit(r, (high, high))


def _get_or_create_ctx():
    if len(ComputeContext.instances) > 0:
        return next(iter(ComputeContext.instances))
    else:
        return create_ctx()


ctx = LazyObject(
    factory=_get_or_create_ctx,
    destructor='stop',
    attrs={
        '__doc__': ComputeContext.__doc__,
        '__class__': ComputeContext,
    },
)
