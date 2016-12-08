Compute Context
===============

.. automodule:: bndl.compute.context
   :no-members:

.. autoclass:: bndl.compute.context.ComputeContext
   :exclude-members: dense
   
   .. attribute:: dense
      
      Create numpy based distributed, partitioned, dense arrays. See
      :class:`dense.arrays <bndl.compute.dense.arrays>`.

.. todo::
   
   The members of dense.sources still don't show signature but they are methods. Their signatures
   *are* rendered correctly in ipython with e.g. `ctx.dense.array?`and also `help(ctx.dense.array)`
   does an okay job ...

.. autoclass:: bndl.compute.dense.arrays
   :undoc-members:
