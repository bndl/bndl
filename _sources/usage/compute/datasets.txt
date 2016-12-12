.. currentmodule:: bndl.compute

Datasets
========

The key datastructure in BNDL Compute is the :class:`Dataset <dataset.Dataset>`. It is
a distributed partitioned collection of values. The :class:`Partitions <dataset.Partition>`
yield an iterable of values which may be arbitrary python objects (which in cases need to be
pickleable / serializable). Datasets can be specialized with optimized partitions e.g.
`numpy.ndarray` instances instead of lists. Datasets can be transformed, reduced (also key-wise),
combined, etc. See further :doc:`./transformations`.


BNDL root data
--------------
A pipepeline (or graph) of transformations on datasets need to start with one or more 'root'
dataset. That is, a dataset which isn't the product of another dataset, e.g. created from an
external system.


Collections
~~~~~~~~~~~
Python iterables can be partitioned and distributed with
:meth:`ctx.collection() <context.ComputeContext.collection>`.
:meth:`ctx.range() <context.ComputeContext.range>` provides a way to get a
distributed and partitioned range of integers. For example::

   >>> ctx.collection('abcd').max()
   'd'
   >>> ctx.collection({'x':1, 'y':2, 'z': 3}).collect()
   [('y', 2), ('x', 1), ('z', 3)]
   >>> ctx.range(10).mean()
   4.5


Files
~~~~~
Files on the filesystem can be read as BNDL Dataset with
:meth:`ctx.files() <context.ComputeContext.files>`. This method can be given a path to a single
file, a list of filepaths or a path of a directory (which is scanned recursively by default). For
example::

   >>> files = ctx.files('.')
   >>> files.filecount
   4461
   >>> 


Dense arrays
~~~~~~~~~~~~
Dense distributed and partitioned Numpy based array datasets can be created through
:meth:`ctx.dense <context.ComputeContext.dense>`::

   ctx.dense.array()
   ctx.dense.range()
   ctx.dense.empty()
   ctx.dense.zeros()
   ctx.dense.ones()


Key-value pair datasets
-----------------------
Python as little to no support for typing variables, the return type of functions, nor for
something like generics. :pep:`484` may change this, but python is still far from a statically
typed language.

Quite a few operators expect key value pairs, e.g.
:meth:`reduce_by_key() <dataset.Dataset.reduce_by_key>` reduces a dataset key-wise.
I.e. the reduction function is applied on all the values with the same key in the dataset. E.g.
in::

   >>> ctx.range(10).map(lambda i: (i//3, i)).aggregate_by_key(sum).collect()
   [(0, 3), (1, 12), (2, 21), (3, 9)]

the mapper function (`lambda i: (i//3, i)`) creates key-value tuples out of a range of integers (0
through 9), but it isn't anotated and can't be inspected that it does that. So BNDL has no good
means to discern datasets with key-value pairs and those that don't. Because of this, operators
like reduce_by_key are available on *any* dataset, regardless of applicability.

When using such operators fails with exceptions like::

   >>> ctx.range(10).aggregate_by_key(sum).values().max()
   2016-12-02 23:42:32,917 - bndl.execute.scheduler -  WARNING - <ComputePartitionTask n9h4eu0f.0 failed> failed on 'localdomain.localhost.worker.27110.0.3' after 1 attempts ... aborting
   Traceback (most recent call last):
   ...
     File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/compute/shuffle.py", line 389, in part_
       return partitioner(key(element))
     File "bndl/util/funcs.pyx", line 44, in bndl.util.funcs._getter (bndl/util/funcs.c:2191)
       return obj[key]
   TypeError: 'int' object is not subscriptable
   
   The above exception was the direct cause of the following exception:
   
   Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
     File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/compute/dataset.py", line 767, in max
       return self.aggregate(partial(max, key=key) if key else max)
   ...
   bndl.rmi.exceptions.InvocationException: An exception was raised on localdomain.localhost.worker.27110.0.3: TypeError

consider whether the the dataset actually contains key-value pairs.


Caching
-------
Datasets can be cached by calling :meth:`Dataset.cache <dataset.Dataset.cache>`. They can be
cached in memory or on disk using various serializations ('json', 'marshal', 'pickle', 'msgpack',
'text' and 'binary') as well as unserialized in memory.

cache
cached
uncache