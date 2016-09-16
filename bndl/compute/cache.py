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
        self.storage_container_factory = StorageContainerFactory(location, serialization, compression)

    def read(self, part):
        container = _caches[part.dset.id][part.idx]
        try:
            data = container.read()
        except FileNotFoundError as e:
            raise KeyError(part.idx) from e
        return data


    def write(self, part, data):
        key = str(part.dset.id), str(part.idx)
        container = self.storage_container_factory(key)
        container.write(data)
        _caches.setdefault(part.dset.id, {})[part.idx] = container


    def clear(self, dset_id, part_idx=None):
        if part_idx:
            _caches[dset_id][part_idx].clear()
            del _caches[dset_id][part_idx]
        else:
            for container in _caches[dset_id].values():
                container.clear()
            _caches[dset_id].clear()
            del _caches[dset_id]
