'''
The BNDL RMI module builds on :mod:`bndl.net` to allow for Remote Method Invocations.

The implementation is rather straight forward: :class:`RMIPeerNode` provides is the means to send
and receive Request and Response Methods. Requests are targeting a method of the remote node by
name. It is simply looked up by ``getattr``.
'''

from .exceptions import *


def direct(remote_method):
    '''
    Decorator to mark a method such that when remotely invoked, no thread is created for its
    execution, but instead is executed within the IO loop. This may optimize performance when the
    result is readily available. However (!) if not (e.g. the method blocks in order to get the
    result) this will stall the IO loop, which _may_ cause issues (e.g. a node becoming
    unresponsive, other nodes assuming the node is lost, etc.).
    '''
    remote_method.__rmi_direct__ = True
    return remote_method


def is_direct(method):
    '''
    Check if a method is marked with the @direct decorator
    '''
    return getattr(method, '__rmi_direct__', False)
