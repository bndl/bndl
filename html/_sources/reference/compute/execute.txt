Execution
=========

Computing :class:`Datasets <bndl.compute.dataset.Dataset>` (when an action is invoked on it) is
performed by :mod:`bndl.execute`. A directed acyclic graph of data sets (one or more source data
sets and the lineage of transformations and combinations) and their partitions is transformed into a
directed acyclic graph of tasks.

These tasks are instances of :class:`ComputePartitionTask <bndl.compute.dataset.ComputePartitionTask>`.
Each partition in a graph of data sets is computed in one task. Partitions in data sets which are
narrow transformations (e.g. map and filter) of their source data set are coalesced into a single
task.

Datasets which are shuffled (e.g. sort, reduce_by_key, join) have all-to-all communication and thus
can't be coalesced into other tasks `across the shuffle`. For example

.. code:: pycon
   
    >>> ctx.range(10, pcount=4).collect()
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

will be computed in four tasks, as would:

.. code:: python
   
    >>> ctx.range(10, pcount=4).map(str).collect()
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
   
When introducing a shuffle, the tasks are split into two groups (stages). I.e.

.. code:: python
   
   >>> kv = ctx.range(10, pcount=4).map(lambda i: (i//3, 1))
   >>> kv.aggregate_by_key(sum, pcount=2).map(str).collect()
   ['(0, 3)', '(2, 3)', '(1, 3)', '(3, 1)']

will result into six tasks: four 'mapper' tasks which would transform the four subranges of 0
through 9 into tuples of (i//3, 1) and 'shuffle' them into two buckets (and sort them by key), and
two reducer tasks which would read from these buckets and take the sum over the values per key.

.. autofunction:: bndl.compute.dataset._compute_part
   
.. autoclass:: bndl.compute.dataset.ComputePartitionTask
.. autoclass:: bndl.compute.dataset.BarrierTask
   