import logging.config
import os.path

from bndl.util.conf import String


dir = String(os.getcwd())  # @ReservedAssignment


def configure_logging(log_dir='/tmp'):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simple': {
                'format': '%(process)d: %(asctime)s   %(name)-20s  l#%(lineno)-3s  %(levelname)-8s  %(message)s'
            }
        },
        'handlers': {
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'simple',
                'filename': os.path.join(log_dir, 'bndl-%s.log' % os.getpid()),
                'encoding': 'utf8'
            },
        },
        'loggers': {
            'asyncio': {
                'level': 'ERROR',
                'handlers': ['file'],
            },
            'bndl': {
                'level': 'DEBUG',
                'handlers': ['file'],
            },
            'bndl.compute': {
                'level': 'DEBUG',
            },
            'bndl.execute': {
                'level': 'DEBUG',
            },
        },
    })
