import asyncio
import itertools
import logging
import sys

from bndl.net.connection import NotConnected
from bndl.net.node import Node
from bndl.net.peer import PeerNode
from bndl.rmi import InvocationException
from bndl.rmi.messages import Response, Request
from bndl.util.aio import async_call
from bndl.util.aio import run_coroutine_threadsafe
from bndl.util.threads import OnDemandThreadedExecutor


from tblib import pickling_support ; pickling_support.install()


logger = logging.getLogger(__name__)


class Invocation(object):
    '''
    Invocation of a method on a PeerNode.
    '''

    def __init__(self, peer, name):
        self.peer = peer
        self.name = name
        self._timeout = None


    def with_timeout(self, timeout):
        self._timeout = timeout
        return self


    def __call__(self, *args, **kwargs):
        request = self._request(*args, **kwargs)
        return run_coroutine_threadsafe(request, self.peer.loop)


    @asyncio.coroutine
    def _request(self, *args, **kwargs):
        request = Request(req_id=next(self.peer._request_ids), method=self.name, args=args, kwargs=kwargs)
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
            method = getattr(self.local, request.method)
        except AttributeError:
            logger.exception('unable to process message for method %s from %s: %s', request.method, self, request)
            exc = sys.exc_info()

        if method:
            try:
                args = (self,) + request.args
                result = yield from async_call(self.loop, self.executor, method, *(args or ()), **(request.kwargs or {}))
            except asyncio.futures.CancelledError:
                logger.debug('handling message from %s cancelled: %s', self, request)
                return
            except Exception as e:
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
            coro = handler.set_result(response)
            if asyncio.iscoroutine(coro):
                yield from coro
        except KeyError:
            logger.warning('Response %r received for unknown request id %r', response, response.req_id)
        except Exception:
            logger.warning('Unable to handle response %r with id %r', response, response.req_id)


    def __getattr__(self, name):
        return Invocation(self, name)


    @asyncio.coroutine
    def disconnect(self, *args, **kwargs):
        yield from super().disconnect(*args, **kwargs)
        for handler in self.handlers.values():
            handler.set_exception(NotConnected())
        self.handlers.clear()


class RMINode(Node):
    PeerNode = RMIPeerNode
