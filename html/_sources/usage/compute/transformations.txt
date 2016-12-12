.. currentmodule:: bndl.compute.dataset.Dataset

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
Some transformations on datasets are much more expensive than other because they require
the elements of the dataset to be 'shuffled'. I.e. when joining two datasets, all elements with the
same key must end up in the same partition (on one machine). This all-to-all communication isn't
cheap per se.

Also collecting a dataset to the driver may be 'expensive' (depending on how much is collected of
course). The major bottleneck here is often the (de)serialization overhead in the driver. As it is
a python process the possibilities for using multiple cores is limited and as such deserialization
is limited to a single core


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
Datasets can be collected to the driver using :meth:`collect` to collect into a list,
:meth:`icollect` to collect into an iterator, :meth:`collect_as_map` and
:meth:`collect_as_set` to collect into a dict or set respectively.

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
with :meth:`collect_as_json` and :meth:`collect_as_pickles`. Collecting as raw
files can be done with :meth:`collect_as_files` (each element must be bytes or str
depending on the mode).


Basic transformations
---------------------
Many transformation are very straight forward in terms of their implementation, e.g.
:meth:`map` and :meth:`filter`. They operate one element or one partition at a
time. Some examples::

   >>> ctx.range(10).map(str).collect()
   ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

   >>> ctx.range(10).filter(lambda i: i < 5).map(float).collect()
   [0.0, 1.0, 2.0, 3.0, 4.0]

   >>> ctx.range(90, 100).map(chr).map(str.isalpha).collect()
   [True, False, False, False, False, False, False, True, True, True]

It may be convenient to call the map or the filter function with *args; :meth:`starmap`
and :meth:`starfilter` are the variadic forms of map and filter::


   dset = tx.collection({'a': 1, 'b': 2, 'c': 3})
   >>> dset.collect()
   [('a', 1), ('c', 3), ('b', 2)]

   >>> dset.starmap(lambda k, v: k + str(v)).collect()
   ['a1', 'c3', 'b2']
   
   >>> dset.starfilter(lambda k, v: k == 'a' or v >= 3).collect()
   [('a', 1), ('c', 3)]
   
A transformation my have multiple outputs which need to be flattened. This can be done with
:meth:`flatmap` e.g. when tokeninzing text::

   >>> lines = ctx.collection([
   ... 'Humpty Dumpty sat on a wall,',
   ... 'Humpty Dumpty had a great fall.',
   ... 'All the king\'s horses and all the king\'s men',
   ... 'Couldn\'t put Humpty together again.',
   ... ], pcount=3)
   
   >>> lines.map(str.split).collect()
   [['Humpty', 'Dumpty', 'sat', 'on', 'a', 'wall,'], ['Humpty', 'Dumpty', 'had', 'a', 'great', 'fall.'], ['All', 'the', "king's", 'horses', 'and', 'all', 'the', "king's", 'men'], ["Couldn't", 'put', 'Humpty', 'together', 'again.']]
   >>> lines.map(str.split).count()
   4
   
   >>> lines.flatmap(str.split).collect()
   ['Humpty', 'Dumpty', 'sat', 'on', 'a', 'wall,', 'Humpty', 'Dumpty', 'had', 'a', 'great', 'fall.', 'All', 'the', "king's", 'horses', 'and', 'all', 'the', "king's", 'men', "Couldn't", 'put', 'Humpty', 'together', 'again.']
   >>> lines.flatmap(str.split).count()
   26
      
:meth:`pluck` is a convenience method to treat the dataset as consisting of sequences
(lists, tuples, etc.) or anything else that supports ``__getitem__`` and get items out of them,
e.g.::

   >>> lines.pluck(2).collect()
   ['m', 'm', 'l', 'u']
   
   >>> lines.pluck([2, 5, 10]).collect()
   [('m', 'y', 'p'), ('m', 'y', 'p'), ('l', 'h', 'n'), ('u', 'n', 'u')]
   
Where :meth:`map` transforms individual elements in a dataset,
:meth:`map_partitions`, :meth:`map_partitions_with_index` and
:meth:`map_partitions_with_part` transform entire partitions. This may be useful for
efficiency or when a transformation intrinsically operates on multiple elements at a time. Note
that the implementations of map and filter use map_partitions in combination with the ``map`` and
``filter`` functions built in python.

When dealing with datasets of key-value pairs, :meth:`keys` and :meth:`values`
allow you to 'pluck' the keys or values respectively from the dataset, e.g.::

   >>> kv = ctx.range(10).map(lambda i: (i, i*3))
   >>> kv.collect()
   [(0, 0), (1, 3), (2, 6), (3, 9), (4, 12), (5, 15), (6, 18), (7, 21), (8, 24), (9, 27)]
   >>> kv.keys().collect()
   [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
   >>> kv.values().collect()
   [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]

Transforming a dataset into one of key-value pairs can be done with :meth:`key_by`,
:meth:`key_by_id` and :meth:`key_by_idx` to add a key and
:meth:`with_value` to add a value::

   >>> ctx.range(5).key_by(str).collect()
   [('0', 0), ('1', 1), ('2', 2), ('3', 3), ('4', 4)]
   
   >>> ctx.range(5).key_by_id().collect()
   [(0, 0), (1, 1), (2, 2), (3, 3), (7, 4)]
   >>> ctx.range(5).key_by_idx().collect()
   [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
   
   >>> ctx.range(5).with_value(str).collect()
   [(0, '0'), (1, '1'), (2, '2'), (3, '3'), (4, '4')]
   >>> ctx.range(5).with_value(1).collect()
   [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)]

Note that key_by_idx requires two pases and should only be used if a 0..n index is required.

Mapping of keys or values can be done with :meth:`map_keys`, :meth:`map_values`,
:meth:`flatmap_values` and :meth:`pluck_values`. Use :meth:`filter_bykey`
and :meth:`filter_byvalue` allow you to filter on the keys or values.

External programs can be used to transform a dataset through :meth:`pipe`::

   >>> chars = lines.map(str.encode).pipe('grep -Po "[a-zA-Z]"').map(bytes.decode).map(str.strip)
   >>> chars.take(10)
   ['H', 'u', 'm', 'p', 't', 'y', 'D', 'u', 'm', 'p']

String or bytes-like elements of a dataset can be concattenate (within a partition) using
:meth:`concat`::

   >>> chars.concat('').collect()
   ['HumptyDumptysatonawall', 'HumptyDumptyhadagreatfall', 'Allthekingshorsesandallthekingsmen', 'CouldntputHumptytogetheragain']


Executing a dataset
-------------------
Occasionally you want to 'compute' a dataset for it's side-effects, e.g. saving it's contents to a
database. For this purpose :meth:`execute` can be used::

   >>> ctx.range(4).map(print).collect()
   1
   0
   2
   3
   [None, None, None, None]
   >>> ctx.range(4).map(print).execute()
   0
   2
   1
   3


Reductions
----------
The transformations in this section all (should) result into a rather small reduction of the total
dataset (e.g. a (few) numbers). Summary statistics can be collected with :meth:`stats`::

   >>> ctx.range(10).stats()
   <Stats count=10, mean=4.5, min=0.0, max=9.0, var=8.25, stdev=2.8722813232690143, skew=0.0, kurt=-1.2242424242424241>

:meth:`mean` is equivalent to the mean property from the stats::

   >>> ctx.range(10).mean()
   4.5

The :meth:`count` and :meth:`sum` have dedicated implementations for efficiency::

   >>> ctx.range(10).count()
   10
   >>> ctx.range(10).sum()
   45

To collect frequencies use :meth:`count_by_value` or a compute a :meth:`histogram`::

   >>> ctx.collection('abcdefgabcdefg').count_by_value()
   Counter({'a': 2, 'e': 2, 'd': 2, 'c': 2, 'f': 2, 'b': 2, 'g': 2})
   
   >>> histogram, bins = ctx.collection('0112358').map(int).histogram(3)
   >>> histogram
   array([4, 2, 1])
   >>> bins
   array([ 0.        ,  2.66666667,  5.33333333,  8.        ])

Counting the number of distinct elements is achieved through :meth:`count_distinct` or
:meth:`count_distinct_approx` for an approximate count (with
`HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_)::

   >>> ctx.collection('abcdefgabcdefg').count_distinct()
   7
   >>> ctx.collection('abcdefgabcdefg').count_distinct_approx()
   7.048292231243761

Collecting the smallest or largest element(s) can be done with :meth:`min`,
:meth:`max`, :meth:`nsmallest` and :meth:`nlargest`::

   >>> ctx.range(10).min()
   0
   >>> ctx.range(10).max()
   9
   >>> ctx.range(10).nsmallest(3)
   [0, 1, 2]
   >>> ctx.range(10).nlargest(3)
   [9, 8, 7]

More complex (less out of the box) aggregations can be implemented with :meth:`aggregate`,
:meth:`combine` and :meth:`reduce` (or their tree-wise implementations :meth:`tree_aggregate`,
:meth:`tree_combine` and :meth:`tree_reduce`)::

   >>> ctx.range(10).aggregate(sum)
   45
   >>> total, count = ctx.range(10).aggregate(lambda x: np.array((sum(x), len(x))), sum)
   >>> total/count
   4.5

   >>> ctx.range(10).combine([], lambda l, e: l + [e], lambda a, b: a + b)
   [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

   >>> ctx.range(1, 10).reduce(lambda a, b: a * b)
   362880

Note that aggregate takes one or two functions; one to aggregate a partition and the second to
aggregate these sub-aggregates. If only one is give, it is used to combine the sub-aggregates as
well.


Distinct elements
-----------------
To filter out any duplicates from a dataset use :meth:`distinct`::

   >>> ctx.collection('abbcccddddeeeeeffffff').distinct().collect()
   ['a', 'e', 'd', 'c', 'b', 'f']


Combinations
------------
Datasets can be combined with :meth:`union`, :meth:`zip`, :meth:`zip_partitions` and
:meth:`product`::

   >>> ctx.range(3).union(ctx.range(10, 13)).collect()
   [0, 1, 2, 10, 11, 12]

   >>> ctx.range(3).zip(ctx.range(10, 13)).collect()
   [(0, 10), (1, 11), (2, 12)]

   >>> ctx.range(3).product(ctx.range(10, 13)).collect()
   [(0, 10), (0, 11), (0, 12), (1, 10), (1, 11), (1, 12), (2, 10), (2, 11), (2, 12)]


Combinations key-wise
---------------------
Datasets can be combined key-wise with :meth:`join`::
   
   >>> a = ctx.range(10)
   >>> b = ctx.range(10).map(lambda i: i//3)
   >>> a.collect(), b.collect()
   ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 0, 0, 1, 1, 1, 2, 2, 2, 3])
      
   >>> a.join(b, key=lambda i: i).collect()
   [(0, [(0, 0), (0, 0), (0, 0)]), (1, [(1, 1), (1, 1), (1, 1)]), (2, [(2, 2), (2, 2), (2, 2)]), (3, [(3, 3)])]
   >>> for key, pairs in a.join(b, key=lambda i: i).collect():
   ...     print(key, pairs)
   ... 
   0 [(0, 0), (0, 0), (0, 0)]
   1 [(1, 1), (1, 1), (1, 1)]
   2 [(2, 2), (2, 2), (2, 2)]
   3 [(3, 3)]

Use :meth:`cogroup` to combine datasets into a dataset with a tuple as values containing the
elements from the datasets with that particular key::

   >>> for key, groups in a.cogroup(b, key=lambda i: i).collect():
   ...     print(key, groups)
   ... 
   0 [[0], [0, 0, 0]]
   8 [[8], []]
   1 [[1], [1, 1, 1]]
   9 [[9], []]
   2 [[2], [2, 2, 2]]
   3 [[3], [3]]
   4 [[4], []]
   5 [[5], []]
   6 [[6], []]
   7 [[7], []]

The examples above use the ``key`` keyword argument, if ommited datasets of key-value pairs are
expected, e.g.::

   >>> a = ctx.collection('abcdefg').key_by_id()
   >>> b = ctx.collection('pqrstuv').key_by_id().map_keys(lambda i: i//2)
   >>> for key, pairs in a.join(b).collect():
   ...     print(key, pairs)
   ... 
   0 [('a', 'p'), ('a', 'r')]
   1 [('c', 'v'), ('c', 't')]
   2 [('e', 'q'), ('e', 's')]
   3 [('g', 'u')]


Key-wise operations
-------------------
Datasets of key-value pairs can be grouped with :meth:`group_by_key` (or :meth:`group_by` and
supply a key function)::

   >>> ctx.range(10).map(lambda i: (i//3, i)).group_by_key().collect()
   [(0, [2, 0, 1]), (1, [5, 3, 4]), (2, [7, 8, 6]), (3, [9])]

Some reductions can also be performed key-wise with :meth:`aggregate_by_key`,
:meth:`combine_by_key` and :meth:`reduce_by_key`::

   >>> chars.map(str.lower).with_value(1).aggregate_by_key(sum).collect()
   [('a', 12), ('e', 7), ('i', 3), ('m', 6), ('u', 7), ('y', 5), ('d', 5), ('h', 8), ('l', 9), ('p', 6), ('t', 13), ('c', 1), ('g', 5), ('k', 2), ('o', 4), ('s', 5), ('w', 1), ('f', 1), ('n', 7), ('r', 3)]

Combine with e.g. :meth:`nlargest` to get a top N::

   >>> chars.map(str.lower).with_value(1).aggregate_by_key(sum).nlargest(3, key=1)
   [('t', 13), ('a', 12), ('l', 9)]

To take a top or bottom N values per key use :meth:`nlargest_by_key` or :meth:`nsmallest_by_key`
respectively::

   >>> ctx.range(10).key_by(lambda i: i//4).nlargest_by_key(2).collect()
   [(0, [3, 2]), (1, [7, 6]), (2, [9, 8])]
   >>> ctx.range(10).key_by(lambda i: i//4).nsmallest_by_key(2).collect()
   [(0, [0, 1]), (1, [4, 5]), (2, [8, 9])]


Shuffles
--------
In order to re-partition a dataset :meth:`shuffle` can be used to move the elements around into
more or less partitions and get the elements in the 'right' partition::

   >>> ctx.range(10, pcount=2).collect(parts=True)
   [range(0, 5), range(5, 10)]
   >>> ctx.range(10, pcount=2).shuffle(pcount=4).collect(parts=True)
   [[0, 4, 8], [1, 5, 9], [2, 6], [3, 7]]
   
.. todo::
   
   Document shuffle options like ``pcount``, ``key``, ``comb``, ``bucket``, ``sort``,
   ``max_mem_pct``, ``block_size_mb``, ``serialization`` and ``compression``. 


Sorting
-------
A dataset can be sorted with :meth:`sort`. First the partition boundaries are computed from which
a range partitioner is created and then the data is shuffled into the sort order. For example::

   >>> ctx.collection('qwerasdfzxcvtyuighjkbnmopl').sort().collect()
   ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


Sampling
--------
Samples from a dataset can be take with :meth:`sample` and :meth:`take_sample`. The former simply
samples the elements with a probability as a 'lazy' dataset, the latter takes an exact number of
samples:

   >>> ctx.range(1000).sample(.01).collect()
   [53, 70, 78, 303, 320, 328, 553, 570, 578, 803, 820, 828]

   >>> ctx.range(1000).take_sample(10)
   [29, 435, 662, 818, 912, 235, 412, 318, 68, 741]
