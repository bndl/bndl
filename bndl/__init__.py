import logging.config
import os.path


if os.path.exists('logging.conf'):
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)


__version_info__ = (0, 3, 4, 'dev1')
__version__ = '.'.join(map(str, __version_info__))
