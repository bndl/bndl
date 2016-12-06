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


Collecting a dataset to the driver
----------------------------------
Peeking
~~~~~~~
During development / exploration it can be very convenient to 'peek' into a dataset. This can be\
done using ``first()`` and ``take(num)`` on a dataset::

   >>> r = ctx.range(100)
   >>> r.first()
   0
   >>> r.take(3)
   [0, 1, 2]


Collecting the total dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Datasets can be collected to the driver using :meth:`collect <bndl.compute.dataset.Dataset.collect>`
to collect into a list, :meth:`icollect <bndl.compute.dataset.Dataset.icollect>` to collect into an
iterator, :meth:`collect_as_map <bndl.compute.dataset.Dataset.collect_as_map>` and
:meth:`collect_as_set <bndl.compute.dataset.Dataset.collect_as_set>` to collect into a dict or set
respectively.

.. code::
   
   >>> r = ctx.range(10)
   >>> r.collect()
   [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
   >>> r.icollect()
   <generator object Dataset.icollect at 0x7f5385f25308>
   >>> next(_)
   0
   >>> ctx.range(97, 107).key_by(chr).collect()
   [('a', 97), ('b', 98), ('c', 99), ('d', 100), ('e', 101), ('f', 102), ('g', 103), ('h', 104), ('i', 105), ('j', 106)]

Datasets can be written to files (one per partition) on the driver node in pickle or json format
with :meth:`collect_as_json <bndl.compute.Dataset.collect_as_json>` and
:meth:`collect_as_pickles <bndl.compute.Dataset.collect_as_pickles>`. Collecting as raw files can
be done with :meth:`collect_as_files <bndl.compute.Dataset.collect_as_files>` (each element must
be bytes or str depending on the mode).
