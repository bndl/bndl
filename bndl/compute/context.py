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

from bndl.compute.accumulate import Accumulator
from bndl.compute.broadcast import broadcast
from bndl.compute.collections import DistributedCollection
from bndl.compute.files import files
from bndl.compute.ranges import RangeDataset
from bndl.execute.context import ExecutionContext


class ComputeContext(ExecutionContext):
    '''
    :class:`ComputeContext` is the main 'handle' into a cluster of BNDL workers from the 'driver' node.

    It provides a means to create partitioned distributed data sets (which can be then transformed and
    combined), broadcast data, create accumulators to collect data into, etc.
    '''
    @property
    def default_pcount(self):
        '''
        The default number of partitions for data sets. This either the `bndl.compute.pcount`
        configuration value or the number of workers (`ctx.worker_count`) times the
        `bndl.execute.concurrency` configuration value (which defaults to 1).
        '''
        pcount = self.conf['bndl.compute.pcount']
        if pcount:
            return pcount
        if self.worker_count == 0:
            self.await_workers()
        return self.worker_count * self.conf['bndl.execute.concurrency']


    def collection(self, collection, pcount=None, psize=None):
        '''
        Create a data set from a python iterable (e.g. a list, dict or iterator).

        Note that iterators are consumed immediately since data sets need to be deterministic in
        their output.

        By default pcount (or `ctx.default_pcount`) partitions are created. pcount must *not* be
        set when psize is set.

        Args:

            collection (iterable): The iterable to partition and distribute.
            pcount (int or None): The number of partition to build.
            psize (int or None): The maximum number of records per partition.

        Example::

            >>> ctx.collection(list('abcd')).nlargest(2)
            ['d', 'c']
        '''
        if isinstance(collection, range):
            return self.range(collection.start, collection.stop, collection.step, pcount=pcount)
        else:
            return DistributedCollection(self, collection, pcount, psize)


    def range(self, start, stop=None, step=1, pcount=None):
        '''
        Create a distributed partitioned data set of a range of numbers.

        Args:
            start (int): The start or stop value (if no stop value is given).
            stop (int): The stop value or None if start is the stop value.
            step (int): The step between each value in the range.
            pcount (int): The number of partitions to partition the range into.

        Examples::

            >>> ctx.range(10).collect()
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            >>> ctx.range(10).glom().collect()
            [range(0, 2), range(2, 5), range(5, 7), range(7, 10)]
            >>> ctx.range(5, 20).glom().map(len).collect()
            [3, 4, 4, 4]
            >> ctx.range(5, 10).stats()
            <Stats count=5, mean=7.0, min=5.0, max=9.0, var=2.0, stdev=1.4142135623730951, skew=0.0, kurt=-1.3>

        '''
        return RangeDataset(self, start, stop, step, pcount)


    def accumulator(self, initial):
        '''
        Create an :class:`Accumulator <bndl.compute.accumulate.Accumulator>` with an initial value.

        Args:
            initial: The initial value of the accumulator.

        Example::

            >>> from bndl.compute import ctx
            >>> accum = ctx.accumulator(0)
            >>> def mapper(i):
            ...     global accum
            ...     accum += i
            ...     return i
            ...
            >>> ctx.range(10).sum()
            45
            >>> accum.value
            0
            >>> ctx.range(10).map(mapper).sum()
            45
            >>> accum.value
            45
            >>> ctx.range(10).map(mapper).sum()
            45
            >>> accum.value
            90
        '''
        accumulator = Accumulator(self, self.node.name, initial)
        self.node.service('accumulate').register(accumulator)
        return accumulator


    broadcast = broadcast
    files = files
