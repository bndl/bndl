'''
The ``bndl.compute`` module provides a means to construct partitioned
:class:`Datasets <bndl.compute.dataset.Dataset>` from a variety of sources and provides operators
which transform and combine these data sets.
'''

import os

from bndl.util import conf
from bndl.util.objects import LazyObject


pcount = conf.Int(desc='The default number of partitions. Then number of '
                       'connected workers * 2 is used if not set.')

worker_count = conf.Int(desc='The number of workers to start when no seeds '
                        'are given when using bndl-compute-shell or '
                        'bndl-compute-workers')


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
