from bndl.net.messages import Message, Field


class Request(Message):
    '''
    A request for a peer RMI node. It contains a request id (to which the :class:`Reponse` must
    refer), the name of the method to be invoked, and the positional and keyword arguments for
    invocation.
    '''
    # int, id of the request
    req_id = Field()
    # str, name of the method to invoke
    method = Field()
    # list or tuple, arguments for the method
    args = Field()
    # dict, keyword arguments for the method
    kwargs = Field()


class Response(Message):
    '''
    A response to a :class:`Request`. It refers to the id of the request and either contains a
    value if the invoked method returned normally or an exception if it raised one.
    '''
    # int, id of the request responded to
    req_id = Field()
    # obj, return value of the method invoked (None if exception raised)
    value = Field()
    # Exception, exception raised by invoked method
    exception = Field()
