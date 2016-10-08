import itertools
import weakref

from bndl.compute.arrays import SourceDistributedArray, DistributedArray
from bndl.compute.broadcast import broadcast, broadcast_pickled
from bndl.compute.collections import DistributedCollection
from bndl.compute.files import files
from bndl.compute.ranges import DistributedRange
from bndl.execute.context import ExecutionContext
from bndl.util.funcs import as_method


class ComputeContext(ExecutionContext):
    instances = weakref.WeakSet()

    def __init__(self, driver, *args, **kwargs):
        super().__init__(driver, *args, **kwargs)
        self._dataset_ids = itertools.count()
        self.instances.add(self)

    @property
    def default_pcount(self):
        if self.worker_count == 0:
            self.await_workers()
        return self.worker_count * self.conf['bndl.execute.concurrency'] * 2

    def collection(self, collection, pcount=None, psize=None):
        if isinstance(collection, range):
            return self.range(collection.start, collection.stop, collection.step, pcount=pcount)
        else:
            return DistributedCollection(self, collection, pcount, psize)

    range = as_method(DistributedRange)
    files = as_method(files)
    broadcast = broadcast
    broadcast_pickled = broadcast_pickled

    array = as_method(SourceDistributedArray)
    empty = as_method(DistributedArray.empty)
    zeros = as_method(DistributedArray.zeros)
    ones = as_method(DistributedArray.ones)
    arange = as_method(DistributedArray.arange)


    def __getstate__(self):
        state = super().__getstate__()
        state.pop('_dataset_ids', None)
        return state
