# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        yielding (
            size:int,
            sender:func(asyncio.BaseEventLoop, asyncio.streams.StreamWriter)
        ) where size is the size of the attachment in bytes and sender is a
        function which is called with the event loop and the writer to which
        the attachment must be written.
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
        raise AttachError("attachments thread local not available")


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
