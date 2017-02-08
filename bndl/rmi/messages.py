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

from bndl.net.messages import Message, Field


class Request(Message):
    '''
    A request for a peer RMI node. It contains a request id (to which the :class:`Response` must
    refer), the name of the method to be invoked, and the positional and keyword arguments for
    invocation.
    '''
    # int, id of the request
    req_id = Field()
    # str, name of the service to invoke a method from
    service = Field()
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
