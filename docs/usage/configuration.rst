Configuration
=============

Various BNDL components can be configured (which often also can be set programmatically as
parameters). At runtime BNDL configuration data is kept in a :class:`bndl.util.conf.Config`
instance of which an instance is available at `bndl.conf`.


Configuration data
------------------
Configuration can be supplied
- directly in python (:class:`bndl.util.conf.Config` supports the `__get/setitem__` protocol).
- through a configuration file
- through the BNDL_CONF environment variable
- command line options


Configuration object
~~~~~~~~~~~~~~~~~~~~
:class:`bndl.util.conf.Config` is a dict like object (it supports the `__get/setitem__` protocol).
For example::

   >>> from bndl import conf
   >>> conf['bndl.compute.worker_count']
   2
   >>> conf['foo'] = 'bar'
   >>> conf
   <Conf {'bndl.compute.worker_count': '2', 'foo': 'bar', 'bndl.net.listen_addresses': 'localhost:1234'}>


Config file
~~~~~~~~~~~
Configuration data is read from bndl.ini / .bndl.ini from the home directory (to whatever ``~``
expands) through ``configparser.ConfigParser``. ini sections and keys are simply joined with a .
For example::

   $ cat bndl.ini 
   [bndl]
   compute.worker_count = 2
   
   [bndl.net]
   listen_addresses = localhost:1234
   
   $ bndl-compute-shell 
   ...
   In [1]: from bndl import conf
   
   In [2]: from bndl import conf
   Out[2]: <Conf {'bndl.compute.worker_count': '2', 'bndl.net.listen_addresses': 'localhost:1234'}>

   In [3]: ctx.worker_count
   Out[3]: 2
   
   In [4]: ctx.node.addresses
   Out[4]: ['localhost:1234']


Environment variable
~~~~~~~~~~~~~~~~~~~~
Configuration data is read from the ``BNDL_CONF`` environment variable. Configuration data can be
supplied as ``key=value other=value foo=bar``. Spacing is parsed through ``shlex.split``. For
example::

   $ BNDL_CONF='bndl.compute.worker_count=3 foo=bar' python
   >>> from bndl.compute import ctx
   >>> ctx.await_workers()
   3
   >>> from bndl import conf
   >>> ctx['foo']
   'bar'



Command line options
~~~~~~~~~~~~~~~~~~~~
`bndl-compute-worker` and `bndl-compute-shell` set:

- :data:`bndl.net.listen_addresses`,
- :data:`bndl.net.seeds` and
- :data:`bndl.compute.worker_count`

through the `--listen-addresses`, `seeds` and `worker-count` flags. See also
:doc:`./compute/getting_started`.


Precedence
~~~~~~~~~~
Configuration data is read in the following order:

- Default values set as global data
- Config files

  - ~/bndl.ini,
  - ~/.bndl.ini,
  - ./bndl.ini and then
  - ./.bndl.ini

- BNDL_CONF environment variable
- Configuration object __init__
- Values set on the configuration object after it's created

I.e. as configuration data is read (updated) in this order, in a way these sources of configuration
data can be considered as layers of defaults / values.


Configuration options
---------------------

The following keys are used throughout BNDL. As this list is manually curated, it *may* become
stale (PR's for improvements are very welcome!).


Networking
~~~~~~~~~~

.. autodata:: bndl.net.listen_addresses
.. autodata:: bndl.net.seeds


Execute
~~~~~~~
BNDL executes tasks on workers (to compute a DAG of datasets and their partitions); if a task fails
``attempts`` times, the job fails.

.. autodata:: bndl.compute.attempts

Workers execute ``concurrency`` tasks simultaneously for each job started.

.. autodata:: bndl.compute.concurrency

.. warning::

   Currently worker-task assignment is orchestrated on a per-job basis. So when multiple jobs are
   executed, workers will run tasks from each job concurrently, regardless of the ``concurrency``
   settings.


Shuffle
~~~~~~~
Shuffles are executed in memory for as long as a workers consumes less than
``bndl.compute.memory.limit`` and the system memory usage is below
``bndl.compute.memory.limit_system``. Over this limit, shuffle data is spilled to disk. Shuffle
data is spilled in blocks (approximately) no larger than ``block_size_mb``.

.. autodata:: bndl.compute.memory.limit
.. autodata:: bndl.compute.memory.limit_system


Broadcast
~~~~~~~~~
Broadcast variables are exchanged in blocks somewhere between:

.. autodata:: bndl.compute.blocks.min_download_size_mb
.. autodata:: bndl.compute.blocks.max_download_size_mb
