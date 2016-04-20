from bndl.net.messages import Message, Field


class Request(Message):
    # int, id of the request
    req_id = Field()
    # str, name of the method to invoke
    method = Field()
    # list or tuple, arguments for the method
    args = Field()
    # dict, keyword arguments for the method
    kwargs = Field()


class Response(Message):
    # int, id of the request responded to
    req_id = Field()
    # obj, return value of the method invoked (None if exception raised)
    value = Field()
    # Exception, exception raised by invoked method
    exception = Field()
