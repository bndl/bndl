Transformations
===============

Datasets can be transformed, reduced (also key-wise), combined, etc. BNDL Compute has a API style
similar to Java streams, Apache Spark, etc. I.e. invoking a method on a dataset creates a new
dataset object with the semantics of the method applied. In fact many operators are have little to
no differences with those in Apache Spark.


Lazy definitions
----------------
Defining datasets and transformations on them is (mostly) lazy. Some preparational work may be
done, but most of the effort is delayed to when an 'action' is called on a dataset such as count,
stats, collect, etc.


Transformations and data movement
---------------------------------
.. todo::
   Narrow vs with shuffle vs collectiong data
   ShuffleRe-organizes values of a dataset accross a cluster of nodes using a partitioner
   I.e. the thing which runs between map and reduce


Basic transformations
---------------------
.. todo::
   Given a source dataset, yield a derived data set given a function which is applied
   The partitions in a derived dataset are often 'lazy' / functions, e.g. map(...) or generators

   - map
   - filter
   - etc.

Reductions
----------
.. todo::

   - aggregate
   - mean
   - etc.

   - HyperLogLog (cardinality counting)


Reductions per key
------------------
.. todo::

   - aggregate_by_key
   - etc.


Combinations
------------
.. todo::
   - union
   - join
   - cogroup
   - etc.


Collecting data
---------------
.. todo::
   - collect
   - icollect
   - first
   - take
