Running a cluster
=================

A BNDL cluster consists of one or more workers and a driver in a fully interconnected network. The
driver node is where your 'program' lives, i.e. the node which puts the workers to work. Typically
1 worker per core is executed.

Driver nodes are (typically) accessable through a :class:`ComputeContext, <bndl.compute.context.ComputeContext>`
(see :doc:`./context`). The basic steps to starting a driver, creating a compute context and
running workers are described in :doc:`./getting_started`.

The easiest way to do start a driver and create a compute context is to create a Compute shell
through the `bndl-compute-shell` command which is available after installation of BNDL. Workers can
be started with the `bndl-compute-workers` command.

.. todo::

   Describe typicall setups, data locality, etc.