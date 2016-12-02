class InvocationException(Exception):
    '''
    Exception indicating a RMI failed. This exception is 'raised from' a 'reconstructed'
    exception as raised in the remote method
    '''

def root_exc(exc):
    '''
    Returns the __cause__ of exc if exc is an InvocationException or just exc otherwise.

    Can be used when both local and remote exceptions need to be handled and their
    semantics are the same (whether the exception was raised locally or on a remote
    worker doesn't matter).

    :param exc: The exception which _might_ be an InvocationException
    '''
    if isinstance(exc, InvocationException):
        return exc.__cause__
    else:
        return exc
