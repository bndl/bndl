# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import atexit
import logging

from bndl.compute.storage import ContainerFactory


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
    def __init__(self, ctx, location, serialization, compression):
        self.ctx = ctx
        self.modify(location, serialization, compression)


    def modify(self, location, serialization, compression):
        self.storage_container_factory = ContainerFactory(location, serialization, compression)


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
        if hasattr(container, 'to_disk'):
            self.ctx.node.memory_manager.add_releasable(
                container.to_disk, container.id, 1, container.size)


    def clear(self, cache_key, obj_key=None):
        if obj_key is not None:
            _caches[cache_key][obj_key].clear()
            del _caches[cache_key][obj_key]
        else:
            for container in _caches[cache_key].values():
                container.clear()
            _caches[cache_key].clear()
            del _caches[cache_key]
