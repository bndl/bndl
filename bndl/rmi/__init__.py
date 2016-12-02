'''
The BNDL RMI module builds on :mod:`bndl.net` to allow for Remote Method Invocations.

The implementation is rather straight forward: :class:`RMIPeerNode` provides is the means to send
and receive Request and Response Methods. Requests are targeting a method of the remote node by
name. It is simply looked up by ``getattr``.
'''

from .exceptions import *
