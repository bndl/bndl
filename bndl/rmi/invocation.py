import asyncio
import logging
import traceback

from bndl.util.aio import run_coroutine_threadsafe
from bndl.rmi.messages import Request


logger = logging.getLogger(__name__)


class InvocationException(Exception):
    '''
    Exception indicating a RMI failed. This exception is 'raised from' a 'reconstructed'
    exception as raised in the remote method
    '''


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
        response_queue = asyncio.queues.Queue()
        self.peer.handlers[request.req_id] = response_queue.put

        logger.debug('remote invocation of %s on %s', self.name, self.peer.name)
        yield from self.peer.send(request)

        try:
            response = (yield from asyncio.wait_for(response_queue.get(), self._timeout))
        except asyncio.futures.CancelledError:
            logger.debug('remote invocation cancelled')
            return None
        except Exception:
            logger.exception('unable to perform remote invocation')
            raise
        finally:
            try:
                del self.peer.handlers[request.req_id]
            except KeyError:
                pass

        if response.exception:
            exc_class, exc, tb = response.exception
            if exc_class:
                exception = exc_class('%s\n---\n%s' %
                                      (str(exc), ''.join(traceback.format_list(tb))))
                raise InvocationException(str(exc)) from exception
            else:
                raise InvocationException('unknown exception')
        else:
            return response.value
