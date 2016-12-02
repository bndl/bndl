Installation
============

BNDL builds are currently distributed through Target Holding's (internal)
`localshop <https://localshop.tgho.nl>`_.

BNDL can be installed through pip::

    pip install bndl

Install development dependencies from a cloned git repository into a virtual environment::

    pyvenv-3.4 venv
    source venv/bin/activate
    git clone ssh://git@stash.tgho.nl:7999/thcluster/bndl.git
    pip install -e bndl[dev]
    

See :doc:`/compatibility` what modules are installed and BNDL is compatible with.

Plugins
-------

BNDL supports plugins, e.g. for connecting with an external system, providing additional
computations, etc. The can be installed like normal python modules through e.g. pip::

   $ pip install bndl_cassandra
   $ pip install bndl_elastic
   $ pip install bndl_ml
   $ pip install bndl_http
