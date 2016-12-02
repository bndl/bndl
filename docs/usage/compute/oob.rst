Broadcasts and accumulators
===========================

Broadcast
Use the same data on every node
Useful for e.g. broadcast joins
a.k.a. lookup table for small relation in hash join

Accumulate
Update a global variable
Useful for:
collecting statistics
communicating results out of band (e.g. from mappers before a shuffle)
