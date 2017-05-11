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

from itertools import chain
from operator import itemgetter
import collections

from cytoolz.itertoolz import partition

from bndl.compute.dataset import Dataset, Partition
from bndl.util.collection import ensure_collection
import numpy as np
import pandas as pd


try:
    from pandas.core.indexes.numeric import NumericIndex
    from pandas.core.indexes.range import RangeIndex
except ImportError:
    from pandas.indexes.numeric import NumericIndex
    from pandas.indexes.range import RangeIndex


def _has_range_idx(frame):
    index = frame.index
    if isinstance(index, RangeIndex):
        return  index[0] == 0 and index[-1] == len(frame) - 1
    elif isinstance(index, NumericIndex):
        return (index.values == range(len(index))).all()
    else:
        return False


def _concat(frames):
    frame = pd.concat(frames)
    if all(map(_has_range_idx, frames)):
        frame.index = RangeIndex(len(frame))
    return frame


class DistributedNDFrame(Dataset):
    def take(self, num):
        heads = self.map_partitions(lambda frame: frame.head(num))._itake_parts()
        try:
            rows = 0
            frames = []
            while rows < num:
                try:
                    head = next(heads)
                except StopIteration:
                    break
                if rows + len(head) > num:
                    head = head.head(num)
                frames.append(head)
                rows += len(head)
                if rows >= num:
                    break
        finally:
            heads.close()
        return _concat(frames)


    def collect(self, parts=False):
        frames = super().collect(True)
        if parts:
            return frames
        return _concat(frames)



class DistributedSeries(DistributedNDFrame):
    def __init__(self, src):
        super().__init__(src.ctx, src)



class DistributedDataFrame(DistributedNDFrame):
    def __init__(self, src, index, columns):
        super().__init__(src.ctx, src)
        self.index = Columns(self, index)
        self.columns = Columns(self, columns)


    @classmethod
    def from_dataset(cls, src, columns=None, samples=10):
        if columns:
            index = []
        else:
            sample = DataFrame(src.take(samples), columns=columns)
            index = [sample.index.name]
            columns = sample.columns
        def to_df(part):
            if isinstance(part, range):
                part = np.arange(part.start, part.stop, part.step)
            if isinstance(part, collections.Iterator):
                part = list(part)
            return  DataFrame(part, columns=columns)
        return cls(src.map_partitions(to_df), index, columns)


    @classmethod
    def from_sample(cls, src, sample):
        index = sample.index
        if isinstance(index, pd.MultiIndex):
            index = [level.name for level in index.levels]
        else:
            index = [index.name]
        return cls(src, index, sample.columns)


    def parts(self):
        return [
            DistributedDataFramePartition(self, i, part)
            for i, part in enumerate(self.src.parts())
        ]


    def __getitem__(self, name):
        if name in self.columns:
            return self.columns.get(name)
        elif name in self.index:
            return self.index.get(name)
        else:
            raise KeyError('No column %s' % name)


    def assign(self, *args, **kwargs):
        df = self
        for name, value in sorted(chain(partition(2, args), kwargs.items()), key=itemgetter(0)):
            # TODO if isinstance(value, np.ndarray):
            #     value = self.ctx.array(value)
            # TODO elif isinstance(value, pd.DataFrame):
            # ...
            # TODO elif not isinstance(value, Dataset):
            #     value = self.ctx.collection(value)
            def assign_values(part, values):
                extended = part.assign(**{name:ensure_collection(values)})
                return extended
            src = df.zip_partitions(value, assign_values)
            df = DistributedDataFrame(src, df.index.copy(), df.columns.copy())
            df.columns.append(pd.Series([Column(df, name)], [name]))
        return df



class Columns(pd.Series):
    def __init__(self, ddf, cols):
        cols = [Column(ddf, col) if col is None or isinstance(col, str) else col for col in cols]
        super().__init__(cols, (col.name for col in cols))



class Column(DistributedDataFrame):
    def __init__(self, ddf, name):
        self.ddf = ddf
        self.name = name
        # TODO fix this up for indices
        src = ddf.map_partitions(lambda part: part[name])
        super().__init__(src, [], [self])

    def __repr__(self, *args, **kwargs):
        return '<Column {c.name} of DDF {c.ddf.id}>'.format(c=self)



class DistributedDataFramePartition(Partition):
    def _compute(self):
        data = self.src.compute()
        if isinstance(data, pd.DataFrame):
            data.__class__ = DataFrame
        return data


class DataFrame(pd.DataFrame):
    def __iter__(self):
        return self.itertuples(name='Row')
