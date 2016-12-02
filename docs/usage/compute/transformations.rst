Transformations
===============


Transformations
Given a source dataset, yield a derived data set given a function which is applied
The partitions in a derived dataset are often 'lazy' / functions, e.g. map(...) or generators

Shuffle
Re-organizes values of a dataset accross a cluster of nodes using a partitioner
I.e. the thing which runs between map and reduce



Summary statistics with Cython
HyperLogLog (cardinality counting)