=========================================
BNDL - Distributed processing with Python
=========================================

BNDL is a python library for map-reduce based distributed processing akin to Apache Spark but is
implemented in python (with a bit of cython).

Master branch build status: |travis| |codecov|

.. |travis| image:: https://travis-ci.org/bndl/bndl.svg?branch=master
   :target: https://travis-ci.org/bndl/bndl

.. |codecov| image:: https://codecov.io/gh/bndl/bndl/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/bndl/bndl/branch/master

---------------------------------------------------------------------------------------------------

BNDL can be installed through pip::

    pip install bndl

The main commands to use BNDL are ``bndl-compute-shell`` to open an interactive shell hooked up to
BNDL workers and ``bndl-compute-workers`` to start workers seperately (e.g. throughout the
cluster).

Obtain a *compute context* in python scripts by importing ``ctx`` from ``bndl.compute.run``::

    from bndl.compute.run import ctx
    print(ctx.range(1000).map(str).map(len).stats())
