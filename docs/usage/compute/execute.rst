Execute
=======

Execute tasks on workers
Tasks run as part of a Job
Jobs may be composed of multiple Stages
BSP model


Workers / cluster
-----------------

Get the number of workers the driver (local node) is connected to::

   >>> ctx.worker_count
   4

Wait for the cluster gossip to stabilize (5 seconds to initial connect ans 60 seconds for the list
to stabilize (peer discovery to stop and the peer list in sync with the others))::

   >>> ctx.await_workers()
   4

Access the list of peers::

   >>> ctx.workers
   [<Peer: localdomain.localhost.worker.29235.0.1>, <Peer: localdomain.localhost.worker.29235.0.3>, <Peer: localdomain.localhost.worker.29235.0.2>, <Peer: localdomain.localhost.worker.29235.0.0>]



Profiling
---------

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


Job and task execution
----------------------

.. todo:: 

   Descibe task states::
   
      [*] --> ready
      [*] --> blocked
      blocked --> ready
      ready --> pending
      pending --> done
      done --> [*]

   and concurrency, retry / attempts, locality, etc.