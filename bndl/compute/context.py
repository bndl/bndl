from bndl.compute.accumulate import Accumulator
from bndl.compute.arrays import SourceDistributedArray, DistributedArray
from bndl.compute.broadcast import broadcast, broadcast_pickled
from bndl.compute.collections import DistributedCollection
from bndl.compute.files import files
from bndl.compute.ranges import RangeDataset
from bndl.execute.context import ExecutionContext
from bndl.util.funcs import as_method


class ComputeContext(ExecutionContext):
    @property
    def default_pcount(self):
        pcount = self.conf['bndl.compute.pcount']
        if pcount:
            return pcount
        if self.worker_count == 0:
            self.await_workers()
        return self.worker_count * self.conf['bndl.execute.concurrency']


    def collection(self, collection, pcount=None, psize=None):
        if isinstance(collection, range):
            return self.range(collection.start, collection.stop, collection.step, pcount=pcount)
        else:
            return DistributedCollection(self, collection, pcount, psize)


    def accumulator(self, initial):
        accumulator = Accumulator(self, self.node.name, initial)
        self.node._register_accumulator(accumulator)
        return accumulator


    broadcast = broadcast
    broadcast_pickled = broadcast_pickled

    def range(self, start, stop=None, step=1, pcount=None):
        return RangeDataset(self, start, stop, step, pcount)

    files = as_method(files)

    array = as_method(SourceDistributedArray)
    empty = as_method(DistributedArray.empty)
    zeros = as_method(DistributedArray.zeros)
    ones = as_method(DistributedArray.ones)
    arange = as_method(DistributedArray.arange)
