MSG_TYPES = {}


class Field(object):
    pass


class MessageType(type):
    def __new__(cls, name, parents, dct):
        dct['__slots__'] = schema = [key for key, value in dct.items()
                                     if isinstance(value, Field)]
        for key in schema:
            dct.pop(key)
        MSG_TYPES[name] = msgtype = super().__new__(cls, name, parents, dct)
        return msgtype



class Message(metaclass=MessageType):
    def __init__(self, **kwargs):
        for k in self.__slots__:
            setattr(self, k, kwargs.get(k))

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__,
                            ', '.join(key + '=' + str(getattr(self, key)) for key in self.__slots__))

    def __msgdict__(self):
        data = {k: getattr(self, k) for k in self.__slots__}
        return (type(self).__name__, data)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        for k in self.__slots__:
            if getattr(self, k, None) != getattr(other, k, None):
                return False
        return True

    @staticmethod
    def load(msg):
        return MSG_TYPES[msg[0]](**msg[1])



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


class Ping(Message):
    pass


class Pong(Message):
    pass
