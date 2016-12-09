Welcome to the BNDL documentation!
==================================

BNDL is a python library for map-reduce based distributed processing akin to Apache Spark.

.. image:: https://travis-ci.org/bndl/bndl.svg?branch=master
   :target: https://travis-ci.org/bndl/bndl

.. image:: https://codecov.io/gh/bndl/bndl/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/bndl/bndl

.. note::

   Get BNDL up and running by following :doc:`install` and :doc:`usage/compute/getting_started`.

The key module for users to interact with BNDL is ``bndl.compute``. This module provides
operators for distributed (partitioned) datasets. Accordingly, the main area of these docs to read
is :doc:`/usage/compute/index`.


Contents
--------

.. toctree::
   :hidden:

   self


.. toctree::
   :maxdepth: 2
   :titlesonly:

   install
   compatibility
   usage/index
   plugins
   reference/index
   issues
   license
   copyright
   changelog



For BNDL developers
-------------------
.. toctree::
   :maxdepth: 2

   development/index
   internals


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

