from bndl.util import conf


pcount = conf.Int(desc='The default number of partitions. Then number of '
                       'connected workers * 2 is used if not set.')

worker_count = conf.Int(desc='The number of workers to start when no seeds '
                        'are given when using bndl-compute-shell or '
                        'bndl-compute-workers')
