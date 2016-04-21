import logging.config
import os.path


def configure_logging(node_name, log_dir='/tmp'):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simple': { 'format': "%(process)d: %(asctime)s - %(name)30s - %(levelname)8s - %(message)s" }
        },
        'handlers': {
            'file': {
                'class': "logging.FileHandler",
                'level': "DEBUG",
                'formatter': "simple",
                'filename': os.path.join(log_dir, 'bndl-' + node_name + ".log"),
                'encoding': "utf8"
            },
            'console': {
                'class': "logging.StreamHandler",
                'level': "DEBUG",
                'formatter': "simple",
            },
        },
        'loggers': {
            'asyncio': {
                'level': "DEBUG",
                'handlers': ["file", "console"],
            },
            'bndl': {
                'level': "DEBUG",
                'handlers': ["file", "console"],
            },
        },
    })


def configure_console_logging():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simple': { 'format': "%(process)d: %(asctime)s   %(name)-20s  l#%(lineno)-3s  %(levelname)-8s  %(message)s" }
        },
        'handlers': {
            'console': {
                'class': "logging.StreamHandler",
                'level': "WARNING",
                'formatter': "simple",
            },
        },
        'loggers': {
            'asyncio': {
                'level': "ERROR",
                'handlers': ["console"],
            },
            'bndl': {
                'level': "WARNING",
                'handlers': ["console"],
            },
            'bndl.compute': {
                'level': "DEBUG",
            },
            'bndl.execute': {
                'level': "DEBUG",
            },
        },
    })
