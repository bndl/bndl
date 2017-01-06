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
