.. currentmodule:: bndl.compute.context.ComputeContext

Broadcasts and accumulators
===========================

Broadcast
---------
Data can be sent to the workers (for use in a mapper, reducer, etc.) with :meth:`broadcast`. This
construct is useful for e.g. broadcast joins a.k.a. lookup table for small relation in hash join::

   >>> tbl = ctx.broadcast(dict(zip(range(4), 'abcd')))
   >>> ctx.range(4).map(lambda i: tbl.value[i]).collect()
   ['a', 'b', 'c', 'd']


Accumulators
------------
Mostly data is communicated back to the driver using
:meth:`Dataset.collect() <bndl.compute.dataset.Dataset.collect>`. However occacially this is
inconvenient when you want to collect data from a transformation before a reduction, e.g. a count::

   >>> lines = ctx.collection([
   ... 'Humpty Dumpty sat on a wall,',
   ... 'Humpty Dumpty had a great fall.',
   ... 'All the king\'s horses and all the king\'s men',
   ... 'Couldn\'t put Humpty together again.',
   ... ])
   
   >>> count = ctx.accumulator(0)
   >>> def chars_from_lines(lines):
   ...     global count
   ...     c = 0
   ...     for line in lines:
   ...         chars = line.split()
   ...         c += len(chars)
   ...         yield from chars
   ...     count += c
   ... 
   >>> chars = lines.flatmap(chars_from_lines)
   >>> chars.with_value(1).aggregate_by_key(sum).nlargest(3, key=1)
   [('t', 13), ('a', 11), ('l', 9)]
   >>> count.value
   116
   >>> chars.count()
   116
   >>> count.value
   232
