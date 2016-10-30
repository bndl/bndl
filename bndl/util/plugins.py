import logging

import pkg_resources


logger = logging.getLogger(__name__)

_plugins = None


def load_plugins():
    global _plugins
    if _plugins is not None:
        return _plugins
    _plugins = []
    for plugin in pkg_resources.iter_entry_points('bndl.plugin'):
        try:
            _plugins.append(plugin.load())
        except:
            logger.warn('Unable to load BNDL plugin %r' % plugin, exc_info=True)
    return _plugins
