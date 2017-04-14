Logging
=======

BNDL uses the python ``logging`` package. To simpify log configuration ``logging.conf`` in the
current working directory is loaded like so::

   logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
   
This is done by default when the ``bndl`` modules is imported (and thus when a worker / driver /
shell is started).

An example log configuration that logs to ``stdout``::

   [loggers]
   keys=root, dataset, scheduler, worker, rmi

   [handlers]
   keys=console

   [formatters]
   keys=simple

   [logger_root]
   level=WARNING
   handlers=console

   [logger_dataset]
   level=INFO
   handlers=console
   qualname=bndl.compute.dataset
   propagate=0

   [logger_scheduler]
   level=DEBUG
   handlers=console
   qualname=bndl.compute.scheduler
   propagate=0

   [logger_worker]
   level=DEBUG
   handlers=console
   qualname=bndl.compute.worker
   propagate=0

   [logger_executor]
   level=DEBUG
   handlers=console
   qualname=bndl.compute.executor
   propagate=0

   [logger_rmi]
   level=INFO
   handlers=console
   qualname=bndl.rmi
   propagate=0

   [handler_console]
   class=StreamHandler
   level=DEBUG
   formatter=simple
   args=(sys.stdout,)

   [formatter_simple]
   format= %(asctime)s - %(name)s - %(levelname)8s - %(message)s
   datefmt=
