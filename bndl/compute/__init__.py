'''
The ``bndl.compute`` module provides a means to construct partitioned
:class:`Datasets <bndl.compute.dataset.Dataset>` from a variety of sources and provides operators
which transform and combine these data sets.
'''

import os

from bndl.util import conf


pcount = conf.Int(desc='The default number of partitions. Then number of '
                       'connected workers * 2 is used if not set.')

worker_count = conf.Int(desc='The number of workers to start when no seeds '
                        'are given when using bndl-compute-shell or '
                        'bndl-compute-workers')


if 'OMP_NUM_THREADS' not in os.environ:
    os.environ['OMP_NUM_THREADS'] = '2'
