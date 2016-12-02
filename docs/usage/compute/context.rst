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


Profile the CPU usage of the cluster::

   >>> ctx.cpu_profiling.start()
   >>> ctx.cpu_profiling.print_stats(10, include=('bndl'))
   
   Clock type: CPU
   Ordered by: totaltime, desc
   
   name                                               ncall         tsub      ttot      tavg      
   bndl/util/threads.py:21 work                       40/36         0.000524  1.552147  0.038804
   bndl/execute/worker.py:83 Worker.execute           40/36         0.001045  1.548825  0.038721
   bndl/execute/worker.py:71 Worker._execute          40/36         0.000953  1.546604  0.038665
   bndl/net/watchdog.py:121 Watchdog._monitor         148           0.005501  0.150095  0.001014
   bndl/net/watchdog.py:140 Watchdog._check           148           0.010773  0.109384  0.000739
   bndl/net/peer.py:63 RMIPeerNode.send               52            0.001444  0.102460  0.001970
   bndl/net/connection.py:132 Connection.send         52            0.002976  0.098297  0.001890
   bndl/rmi/node.py:135 RMIPeerNode._send_response    40            0.000636  0.096662  0.002417
   bndl/net/watchdog.py:156 Watchdog._check_peer      592           0.015429  0.092849  0.000157
   bndl/net/serialize.py:51 dump 
   >>> ctx.cpu_profiling.stop()
