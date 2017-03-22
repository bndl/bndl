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

from bndl.compute.exceptions import *
from bndl.util import conf
from bndl.util.objects import LazyObject


pcount = conf.Int(desc='The default number of partitions. Then number of connected executors is '
                       'used if not set.')

executor_count = conf.Int(os.cpu_count(), desc='The number of executors to start when no seeds are '
                               'given when using bndl-compute-shell or bndl-compute-worker')

concurrency = conf.Int(1, desc='the number of tasks which can be scheduled at an executor process '
                               'at the same time')

attempts = conf.Int(1, desc='The number of times a task is attempted before the job is cancelled')


if 'OMP_NUM_THREADS' not in os.environ:
    os.environ['OMP_NUM_THREADS'] = '2'


def _get_or_create_ctx():
    from bndl.compute.context import ComputeContext
    if len(ComputeContext.instances) > 0:
        return next(iter(ComputeContext.instances))
    else:
        from bndl.compute.run import create_ctx
        return create_ctx()


ctx = LazyObject(_get_or_create_ctx, 'stop')
