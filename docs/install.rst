Installing
==========

BNDL builds are currently distributed through Target Holding's (internal)
`localshop <https://localshop.tgho.nl>`_.

BNDL can be installed through pip:

.. code:: bash

    pip install bndl

Install development dependencies with:

.. code:: bash 

    pip install bndl[dev]
    

Plugins
-------

.. todo::

    Add plugin install docs


Install with pip

.. code:: bash

   $ pip install bndl

   # plugins extend Dataset and ComputeContext with various methods
   $ pip install bndl_cassandra
   $ pip install bndl_elastic

   # normal module, import and use
   $ pip install bndl_ml




Compatibility
-------------

BNDL is compatible with python 3.4 and 3.5.

.. todo::
   
   .. program-output:: python3 -c 'import setup ; print("\n".join(setup.install_requires))'
      :cwd: ..
