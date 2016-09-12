from functools import reduce
from itertools import chain
from operator import itemgetter
import collections

from bndl.compute.dataset import Dataset, Partition
from bndl.util.collection import ensure_collection
from cytoolz.itertoolz import partition
from pandas.indexes.range import RangeIndex
import numpy as np
import pandas as pd


def _has_range_idx(frame):
    return isinstance(frame.index, RangeIndex) and frame.index[0] == 0 and frame.index[-1] == len(frame) - 1


def _concat(frames):
    frame = pd.concat(frames)
    if all(map(_has_range_idx, frames)):
        frame.index = RangeIndex(len(frame))
    return frame


class DistributedNDFrame(Dataset):
    def take(self, num):
        heads = self.map_partitions(lambda frame: frame.head(num)).icollect(eager=False, parts=True)
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
            if isinstance(value, np.ndarray):
                value = self.ctx.array(value)
            # TODO elif isinstance(value, pd.DataFrame):
            # ...
            elif not isinstance(value, Dataset):
                value = self.ctx.collection(value)
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
    def _materialize(self, ctx):
        data = self.src.materialize(ctx)
        if isinstance(data, pd.DataFrame):
            data.__class__ = DataFrame
        return data


class DataFrame(pd.DataFrame):
    def __iter__(self):
        return self.itertuples(name='Row')
