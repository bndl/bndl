import logging.config
import os.path


if os.path.exists('logging.conf'):
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
