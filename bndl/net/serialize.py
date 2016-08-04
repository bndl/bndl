import threading

from bndl.util import serialize


_ATTACHMENTS = threading.local()


class AttachError(Exception):
    '''
    Raised when attempting to use attach(...) outside the context of pickling
    as provided by dump
    '''


def attach(key, att):
    '''
    Add an attachment to the message currently being serialized (pickled)
    :param key: bytestring
        to lookup the attachment on deserialization
    :param att: obj
        The object to attach.
        When used in the context of bndl.net obj should be a contextmanager
        yielding (size:int, sender:func(asyncio.streams.StreamWriter)) where
        size is the size of the attachment in bytes and sender is a function
        which is called with the writer to which the attachment must be
        written.
        This construct can be used to e.g. use sendfile to more efficiently
        send data outside of pickle (in the sendfile case, without a bunch of
        system calls for reading the file and writing to the socket and without
        a bunch of copies).
    '''
    try:
        attachments = getattr(_ATTACHMENTS, 'v')
        if key in attachments:
            return AttachError("key conflict in attaching key " + str(key))
        attachments[key] = att
    except AttributeError:
        _ATTACHMENTS.v = {key: att}


def attachment(key):
    try:
        return getattr(_ATTACHMENTS, 'v')[key]
    except AttributeError:
        return AttachError("attachments thread local not available")


def dump(obj):
    marshalled, serialized = serialize.dumps(obj)
    attachments = getattr(_ATTACHMENTS, 'v', None)
    if attachments:
        del _ATTACHMENTS.v
    return marshalled, serialized, attachments


def load(marshalled, msg, attachments):
    setattr(_ATTACHMENTS, 'v', attachments)
    value = serialize.loads(marshalled, msg)
    del _ATTACHMENTS.v
    return value
