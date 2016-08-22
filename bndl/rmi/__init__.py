
class InvocationException(Exception):
    '''
    Exception indicating a RMI failed. This exception is 'raised from' a 'reconstructed'
    exception as raised in the remote method
    '''
