Datasets
========

Dataset
A distributed partitioned collection of values
Values may be any python object

Partition
Partitions 'contain' iterables (the 'subcollections')
Datasets can be specialized with optimized partitions
e.g. numpy.ndarray


BNDL 'root' data sets
---------------------
Currently supported:
Python iterables
Files on the filesystem of the driver
Cassandra table scans and friends
Elastic search queries


