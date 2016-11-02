from contextlib import contextmanager
import logging


logger = logging.getLogger(__name__)


@contextmanager
def catch(*ignore):
    try:
        yield
    except Exception as exc:
        if ignore and not any(isinstance(exc, i) for i in ignore):
            raise exc
        else:
            logger.debug('silenced exception', exc_info=True)
