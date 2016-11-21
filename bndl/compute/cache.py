import atexit
import logging

from bndl.compute.storage import StorageContainerFactory


logger = logging.getLogger(__name__)


_caches = {}


@atexit.register
def clear_all():
    for cache in _caches.values():
        for container in cache.values():
            container.clear()
        cache.clear()
    _caches.clear()


class CacheProvider(object):
    def __init__(self, location, serialization, compression):
        self.modify(location, serialization, compression)


    def modify(self, location, serialization, compression):
        self.storage_container_factory = StorageContainerFactory(location, serialization, compression)


    def read(self, cache_key, obj_key):
        container = _caches[cache_key][obj_key]
        try:
            data = container.read()
        except FileNotFoundError as e:
            raise KeyError(obj_key) from e
        return data


    def write(self, cache_key, obj_key, data):
        key = str(cache_key), str(obj_key)
        container = self.storage_container_factory(key)
        container.write(data)
        _caches.setdefault(cache_key, {})[obj_key] = container


    def clear(self, cache_key, obj_key=None):
        if obj_key is not None:
            _caches[cache_key][obj_key].clear()
            del _caches[cache_key][obj_key]
        else:
            for container in _caches[cache_key].values():
                container.clear()
            _caches[cache_key].clear()
            del _caches[cache_key]
