import threading

from bndl.util import serialize


_attachments = threading.local()


class AttachError(Exception):
    '''
    Raised when attempting to use attach(...) outside the context of pickling
    as provided by _dump_with_attachments
    '''

def attach(key, attachment):
    '''
    Add an attachment to the message currently being serialized (pickled)
    :param key: bytestring
        to lookup the attachment on deserialization
    :param attachment: TODO 
    '''
    try:
        attachments = getattr(_attachments, 'v')
    except AttributeError:
        attachments = _attachments.v = {}
    if key in attachments:
        return AttachError("key conflict in attaching key " + str(key))
    attachments[key] = attachment


def attachment(key):
    try:
        attachments = getattr(_attachments, 'v')
    except AttributeError:
        return AttachError("attachments thread local not available")
    return attachments[key]


def dump(obj):
    marshalled, serialized = serialize.dumps(obj)
    attachments = getattr(_attachments, 'v', {})
    _attachments.v = {}
    return marshalled, serialized, attachments


def load(marshalled, msg, attachments):
    setattr(_attachments, 'v', attachments)
    obj = serialize.loads(marshalled, msg)
    _attachments.v = {}
    return obj
