import asyncio
import itertools
import logging
import sys
import traceback

from bndl.net.connection import NotConnected
from bndl.net.node import Node
from bndl.net.peer import PeerNode
from bndl.rmi.invocation import Invocation
from bndl.rmi.messages import Response, Request
from bndl.util.aio import async_call
from bndl.util.threads import OnDemandThreadedExecutor


logger = logging.getLogger(__name__)


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
            logger.warning('unable to deliver response %s on connection %s (not connected)', response.req_id, self)
        except asyncio.futures.CancelledError:
            logger.warning('unable to deliver response %s on connection %s (cancelled)', response.req_id, self)
        except Exception:
            logger.exception('unable to send response')
            exc = sys.exc_info()

        if exc:
            response.value = None
            exc_class, exc, tback = exc
            tback = traceback.extract_tb(tback)
            response.exception = exc_class, exc, tback
            try:
                yield from self.send(response)
            except Exception:
                logger.exception('unable to send exception')


    @asyncio.coroutine
    def _handle_response(self, response):
        try:
            handler = self.handlers.pop(response.req_id)
            coro = handler(response)
            if asyncio.iscoroutine(coro):
                yield from coro
        except KeyError:
            logger.warning('Response %r received for unknown request id %r', response, response.req_id)
        except Exception:
            logger.warning('Unable to handle response %r with id %r', response, response.req_id)


    def __getattr__(self, name):
        return Invocation(self, name)



class RMINode(Node):
    PeerNode = RMIPeerNode
