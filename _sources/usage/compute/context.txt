Context
=======

:class:`bndl.compute.context.ComputeContext` is the entry point into a cluster of BNDL workers
from the 'driver' node. It provides a means to create partitioned distributed data sets (which can
be then transformed and combined), broadcast data, create accumulators to collect data into, etc.

See :doc:`./getting_started` for creating a compute context.


Datasets
--------
See :doc:`./datasets` for more on data sets. ComputeContext is the main handle into creating some
data sets (although most functionality is enclosed in the implementations of
:class:`bndl.compute.dataset.Dataset`. Some examples::

   >>> r = ctx.range(10)
   >>> r
   <RangeDataset 38s8n3ym>
   >>> r.collect()
   [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
   
   >>> c = ctx.collection('The quick brown fox jumps over the lazy dog')
   >>> c
   <DistributedCollection 3kuycn22>
   >>> for char, count in c.with_value(1).aggregate_by_key(sum).nlargest(4, key=1):
   ...     print(char, count)
   ... 
     8
   o 4
   e 3
   u 2

   >>> f = ctx.files('.') # key value pairs of (filename:str, contents:bytes)
   >>> f
   <RemoteFilesDataset o4p97hrt>
   >>> f.values().map(len).stats()
   <Stats count=4495, mean=23657.52413793103, min=0.0, max=5709296.0, var=45209392506.7967, stdev=212625.00442515386, skew=17.166050243279887, kurt=356.5405806659577>
   

Distributed global variables
----------------------------
On occasions it's convinient to share (broadcast) some data with all workers (and not have it
serialized and set for every task again). Or the opposite: let every worker (e.g. in a mapper or
reducer task) send data 'out of band' to (accumulate on) the driver. See :doc:`oob` for more on
these topics.


Workers / cluster and Profiling
-------------------------------
:class:`ComputeContext <bndl.compute.context.ComputeContext>` inherits from
:class:`ExecuteContext <bndl.execute.context.ExecuteContext>` and thus exposes functions and
properties for e.g. waiting for workers and profiling see :doc:`../execute` for more.
