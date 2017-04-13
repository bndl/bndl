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

'''
The BNDL RMI module builds on :mod:`bndl.net` to allow for Remote Method Invocations.

The implementation is rather straight forward: :class:`RMIPeerNode` provides is the means to send
and receive Request and Response Methods. Requests are targeting a method of the remote node by
name. It is simply looked up by ``getattr``.
'''

from functools import partial
import asyncio
import itertools
import logging
import sys

from bndl.net.aio import run_coroutine_threadsafe, async_call
from bndl.net.connection import NotConnected
from bndl.net.messages import Message, Field
from bndl.net.node import Node
from bndl.net.peer import PeerNode
from bndl.util.threads import OnDemandThreadedExecutor


from tblib import pickling_support ; pickling_support.install()


logger = logging.getLogger(__name__)


def direct(remote_method):
    '''
    Decorator to mark a method such that when remotely invoked, no thread is created for its
    execution, but instead is executed within the IO loop. This may optimize performance when the
    result is readily available. However (!) if not (e.g. the method blocks in order to get the
    result) this will stall the IO loop, which _may_ cause issues (e.g. a node becoming
    unresponsive, other nodes assuming the node is lost, etc.).
    '''
    remote_method.__rmi_direct__ = True
    return remote_method


def is_direct(method):
    '''
    Check if a method is marked with the @direct decorator
    '''
    return getattr(method, '__rmi_direct__', False)



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


class Invocation(object):
    '''
    Invocation of a method on a PeerNode.
    '''

    def __init__(self, peer, service, name):
        self.peer = peer
        self.service = service
        self.name = name
        self._timeout = None


    def with_timeout(self, timeout):
        '''
        Apply a time out in performing the remote method invocation. When the time out expires
        ``concurrent.futures.TimeoutError`` is raised.
        :param timeout:
        '''
        self._timeout = timeout
        return self


    def __call__(self, *args, **kwargs):
        request = self.request(*args, **kwargs)
        return run_coroutine_threadsafe(request, self.peer.loop)


    @asyncio.coroutine
    def request(self, *args, **kwargs):
        request = Request(req_id=next(self.peer._request_ids),
                          service=self.service, method=self.name,
                          args=args, kwargs=kwargs)

        response_future = asyncio.Future(loop=self.peer.loop)
        self.peer.handlers[request.req_id] = response_future

        logger.debug('remote invocation of %s on %s', self.name, self.peer.name)
        yield from self.peer.send(request)

        try:
            response = (yield from asyncio.wait_for(response_future, self._timeout, loop=self.peer.loop))
        except asyncio.futures.CancelledError:
            logger.debug('remote invocation cancelled')
            return None
        except asyncio.futures.TimeoutError:
            raise
        except NotConnected:
            logger.info('%s not connected, unable to perform remote invocation of %s' %
                        (self.peer.name, self.name))
            raise
        except Exception:
            logger.exception('unable to perform remote invocation of %s on %s' % (
                             self.name, self.peer.name))
            raise
        finally:
            try:
                del self.peer.handlers[request.req_id]
            except KeyError:
                pass

        if response.exception:
            exc_class, exc, tback = response.exception
            if not exc:
                exc = exc_class()
            source = exc.with_traceback(tback)
            iexc = InvocationException('An exception was raised on %s: %s' %
                                       (self.peer.name, exc_class.__name__))
            raise iexc from source
        else:
            return response.value



class Service(object):
    def __init__(self, peer, name):
        self.peer = peer
        self.name = name


    def __getattr__(self, name):
        return Invocation(self.peer, self.name, name)


    def __repr__(self):
        return '<RmiService %r>' % self.name



class RMIPeerNode(PeerNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_ids = itertools.count()
        self.handlers = {}
        self.executor = OnDemandThreadedExecutor()


    @asyncio.coroutine
    def _dispatch(self, msg):
        if isinstance(msg, Request):
            yield from self._handle_request(msg)
        elif isinstance(msg, Response):
            yield from self._handle_response(msg)
        else:
            yield from super()._dispatch(msg)


    @asyncio.coroutine
    def _handle_request(self, request):
        method = None
        result = None
        exc = None

        try:
            service = self.local.services[request.service]
            method = getattr(service, request.method)
        except KeyError:
            logger.info('unable to process message for unknown service %r from %r: %r',
                        request.service, self, request)
            exc = sys.exc_info()
        except AttributeError:
            logger.info('unable to process message for unknown method %r.%r from %r: %r',
                        request.service, request.method, self, request)
            exc = sys.exc_info()

        if method:
            try:
                args = (self,) + request.args
                kwargs = request.kwargs or {}

                if asyncio.iscoroutinefunction(method):
                    result = (yield from method(*args, **kwargs))
                elif is_direct(method):
                    result = method(*args, **kwargs)
                else:
                    if kwargs:
                        method = partial(method, **kwargs)
                    result = (yield from self.loop.run_in_executor(self.executor, method, *args))
            except asyncio.futures.CancelledError:
                logger.debug('handling message from %s cancelled: %s', self, request)
                return
            except Exception:
                logger.debug('unable to invoke method %s', request.method, exc_info=True)
                exc = sys.exc_info()

        yield from self._send_response(request, result, exc)


    @asyncio.coroutine
    def _send_response(self, request, result, exc):
        response = Response(req_id=request.req_id)

        try:
            if not exc:
                response.value = result
                yield from self.send(response)
        except NotConnected:
            logger.info('unable to deliver response %s on connection %s (not connected)', response.req_id, self)
        except asyncio.futures.CancelledError:
            logger.info('unable to deliver response %s on connection %s (cancelled)', response.req_id, self)
        except Exception:
            logger.exception('unable to send response')
            exc = sys.exc_info()

        if exc:
            response.value = None
            response.exception = exc
            try:
                yield from self.send(response)
            except Exception:
                logger.exception('unable to send exception')


    @asyncio.coroutine
    def _handle_response(self, response):
        try:
            handler = self.handlers.pop(response.req_id)
            handler.set_result(response)
        except KeyError:
            logger.info('Response %r received for unknown request id %r', response, response.req_id)
        except Exception:
            logger.warning('Unable to handle response %r with id %r', response, response.req_id)


    def service(self, name):
        return Service(self, name)


    @asyncio.coroutine
    def disconnect(self, *args, **kwargs):
        yield from super().disconnect(*args, **kwargs)
        for handler in self.handlers.values():
            handler.set_exception(NotConnected())
        self.handlers.clear()



class RMINode(Node):
    '''
    A :class:`bndl.net.Node` which expects it's peers to support remote method invocation as
    implemented in :class:`RMIPeerNode`.
    '''
    PeerNode = RMIPeerNode

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.services = {}


    def service(self, name):
        try:
            return self.services[name]
        except KeyError:
            raise KeyError('Unknown service %r' % name)


    @asyncio.coroutine
    def start(self):
        for svc in self.services.values():
            if hasattr(svc, 'start'):
                yield from async_call(self.loop, None, svc.start)
        yield from super().start()


    @asyncio.coroutine
    def stop(self):
        for svc in self.services.values():
            if hasattr(svc, 'stop'):
                yield from async_call(self.loop, None, svc.stop)
        yield from super().stop()

