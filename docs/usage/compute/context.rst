Context
=======

:class:`bndl.compute.context.ComputeContext` is the main 'handle' into a cluster of BNDL workers
from the 'driver' node. It provides a means to create partitioned distributed data sets (which can
be then transformed and combined), broadcast data, create accumulators to collect data into, etc.

See :doc:`./getting_started` for creating a compute context.

Example usage:
--------------

Get the number of workers the driver (local node) is connected to::

   >>> ctx.worker_count
   4
   
Wait for the cluster gossip to stabilize::

   >>> ctx.await_workers()
   4   

Create some data sets::

   >>> r = ctx.range(10)
   >>> r
   <RangeDataset 38s8n3ym>
   
   >>> c = ctx.collection('abcdefghijkl')
   >>> c
   <DistributedCollection 3kuycn22>

   >>> f = ctx.files('.')
   >>> f
   <RemoteFilesDataset o4p97hrt>

See :doc:`./datasets` for more on data sets.


Profile the CPU and memory usage of the cluster::

   >>> ctx.cpu_profiling.start()
   >>> ctx.memory_profiling.start()
   >>> ctx.cpu_profiling.print_stats(3, include=('bndl'))
   
   Clock type: CPU
   Ordered by: totaltime, desc
   
   name                                               ncall         tsub      ttot      tavg      
   bndl/util/threads.py:21 work                       40/36         0.000524  1.552147  0.038804
   bndl/execute/worker.py:83 Worker.execute           40/36         0.001045  1.548825  0.038721
   bndl/execute/worker.py:71 Worker._execute          40/36         0.000953  1.546604  0.038665
   >>> ctx.memory_profiling.print_top(limit=3)
   #1 <frozen importlib._bootstrap_external>:484 136.0 KiB
   #2 home/frens-jan/Workspaces/ext/cpython3.5/Lib/abc.py:133 14.9 KiB
       cls = super().__new__(mcls, name, bases, namespace)
   #3 home/frens-jan/Workspaces/ext/cpython3.5/Lib/tracemalloc.py:68 10.2 KiB
       class StatisticDiff:
   158 other: 199.5 KiB
   Total allocated size: 360.7 KiB
   <tracemalloc.Snapshot object at 0x7f89686c7898>
   >>> ctx.cpu_profiling.stop()
   >>> ctx.memory_profiling.stop()
