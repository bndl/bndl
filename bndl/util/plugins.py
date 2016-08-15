import logging

import pkg_resources


logger = logging.getLogger(__name__)


def load_plugins():
    plugins = []
    for plugin in pkg_resources.iter_entry_points('bndl.plugin'):
        try:
            plugins.append(plugin.load())
        except:
            logger.warn('Unable to load BNDL plugin %r' % plugin, exc_info=True)
    return plugins