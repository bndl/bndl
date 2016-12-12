Known issues
============

 * ``bndl.compute`` uses ``weakref`` (as does ``asyncio``) and thus suffers from https://bugs.python.org/issue26617
   and as such is prone to hang with python < 3.4.4 and < 3.5.2 with a fair amount of 'work'. Especially when the
   jobs involve shuffles.
 * yappi 0.94 has changed in a quite drastic way and isn't supported yet