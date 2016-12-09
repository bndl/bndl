BNDL
====

Bundle compute resources in Python across cores and machines.

.. image:: https://travis-ci.org/bndl/bndl.svg?branch=master
   :target: https://travis-ci.org/bndl/bndl

.. image:: https://codecov.io/gh/bndl/bndl/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/bndl/bndl


Install
-------

BNDL can be installed through pip:

.. code:: python

    pip install bndl

Install dependencies with:

.. code:: python

    pip install bndl[dev]

Usage
-----

Shell
~~~~~

The main commands to use BNDL are:

-  ``bndl-compute-shell`` and
-  ``bndl-compute-workers``

Script
~~~~~~

Python scripts use ``bndl.compute.run``, e.g.:

.. code:: python

    from bndl.compute.run import ctx
    print(ctx.range(1000).map(str).map(len).stats())
