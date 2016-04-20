import itertools

from bndl.compute.broadcast import broadcast
from bndl.compute.dataset.arrays import SourceDistributedArray, DistributedArray
from bndl.compute.dataset.collections import DistributedCollection
from bndl.compute.dataset.files import DistributedFiles
from bndl.compute.dataset.ranges import DistributedRange
from bndl.execute.context import ExecutionContext
from bndl.util.funcs import as_method


class ComputeContext(ExecutionContext):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataset_ids = itertools.count()

    @property
    def default_pcount(self):
        return self.worker_count

    def collection(self, c, pcount=None, psize=None):
        if isinstance(c, range):
            return self.range(c.start, c.stop, c.step, pcount=pcount)
        else:
            return DistributedCollection(self, c, pcount, psize)

    range = as_method(DistributedRange)
    files = as_method(DistributedFiles)
    broadcast = broadcast

    array = as_method(SourceDistributedArray)
    empty = as_method(DistributedArray.empty)
    zeros = as_method(DistributedArray.zeros)
    ones = as_method(DistributedArray.ones)
    arange = as_method(DistributedArray.arange)


    def __getstate__(self):
        state = super().__getstate__()
        state.pop('_dataset_ids', None)
        return state
