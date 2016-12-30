import logging.config
import os.path

from bndl.util.conf import Config
from bndl.util.objects import LazyObject


conf = LazyObject(Config)


if os.path.exists('logging.conf'):
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)


__version_info__ = (0, 4, 0, 'dev1')
__version__ = '.'.join(map(str, __version_info__))
