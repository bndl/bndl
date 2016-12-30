Remote Method Invocation
========================
On top of ``bnd.net`` a small module is layered which allows for remote method invocation. It's
fairly simple, any not otherwise resolvable attribute on an
:class:`RMIPeerNode <bndl.rmi.node.RMIPeerNode>` object is considered a remote method. When called
the name of the member and the arguments of the call are sent to the other node, the member is
'resolved' through ``getattr(self, name)`` and then invoked with the ``*args**`` and ``**kwargs``
of the call.

Obviously the member should exist on the :class:`RMINode bndl.rmi.node.RMINode`
receiving the invocation. To execute arbitrary functions on the remote node run worker/driver nodes
in the cluster from the ``bndl.compute`` or ``bndl.execute`` modules; e.g. by running the
``bndl-compute-shell`` or ``bndl-compute-workers`` commands. Such nodes provide the
``execute(...)``` method, e.g.::

   >>> from bndl.compute import ctx
   >>> ctx.await_workers()
   4
   >>> ctx.workers[0].execute(lambda: 42).result()
   42
   
Invoking a remote method returns a ``concurrent.futures.Future``. Call ``.result()`` on it the get
the response. Use ``.with_timeout(n)`` to wait at most `n` seconds for a response. A
``concurrent.futures.TimeoutError`` will be raised if that happens::
   
   >>> ctx.workers[0].execute.with_timeout(1)(lambda: 42).result()
   42
   >>> ctx.workers[0].execute.with_timeout(0)(lambda: 42).result()
   Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
     File "/home/frens-jan/Workspaces/ext/cpython3.5/Lib/concurrent/futures/_base.py", line 405, in result
       return self.__get_result()
     File "/home/frens-jan/Workspaces/ext/cpython3.5/Lib/concurrent/futures/_base.py", line 357, in __get_result
       raise self._exception
   2016-12-05 00:33:51,068 - bndl.rmi.node -  WARNING - Response <Response exception=None, req_id=1, value=42> received for unknown request id 1
     File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/util/aio.py", line 64, in task_done
       future.set_result(task.result())
     File "/home/frens-jan/Workspaces/ext/cpython3.5/Lib/asyncio/futures.py", line 292, in result
       raise self._exception
     File "/home/frens-jan/Workspaces/ext/cpython3.5/Lib/asyncio/tasks.py", line 239, in _step
       result = coro.send(None)
     File "/home/frens-jan/Workspaces/tgho/bndl/bndl/bndl/rmi/node.py", line 58, in _request
       response = (yield from asyncio.wait_for(response_future, self._timeout, loop=self.peer.loop))
     File "/home/frens-jan/Workspaces/ext/cpython3.5/Lib/asyncio/tasks.py", line 404, in wait_for
       raise futures.TimeoutError()
   concurrent.futures._base.TimeoutError
   
   
