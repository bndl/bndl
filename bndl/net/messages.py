_msgtypes = {}


class Field(object):
    pass


class MessageType(type):
    def __new__(cls, name, parents, dct):
        dct['__slots__'] = schema = [k for k, v in dct.items() if isinstance(v, Field)]
        for k in schema:
            dct.pop(k)
        _msgtypes[name] = msgtype = super().__new__(cls, name, parents, dct)
        return msgtype



class Message(metaclass=MessageType):
    def __init__(self, **kwargs):
        self.__setstate__(kwargs)

    def __getstate__(self):
        return {k:getattr(self, k) for k in self.__slots__ if getattr(self, k) is not None}

    def __setstate__(self, state):
        for k in self.__slots__:
            setattr(self, k, state.get(k))

    def __str__(self):
        return (self.__class__.__name__ + '(' +
            ', '.join(k + '=' + str(getattr(self, k)) for k in self.__slots__)
        + ')')

    def __msgdict__(self):
        d = {k:getattr(self, k) for k in self.__slots__}
        return (type(self).__name__, d)

    @staticmethod
    def load(msg):
        return _msgtypes[msg[0]](**msg[1])



class Hello(Message):
    # str, name of node
    name = Field()
    # str, cluster of node
    cluster = Field()
    # str, type of node
    node_type = Field()
    # list or set of str, addresses at which the node can be reached
    addresses = Field()


class Discovered(Message):
    # list of name, addresses tuples
    peers = Field()


class Disconnect(Message):
    # str for debug perposes
    reason = Field()
