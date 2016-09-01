from functools import reduce

from bndl.compute.dataset import Dataset, Partition
import pandas as pd


def _pairwise_append(a, b):
    return a.append(b)


def combine_dataframes(dfs):
    return reduce(_pairwise_append, dfs)



class DataFrame(pd.DataFrame):
    def __iter__(self):
        return self.itertuples(name='Row')



class Columns(pd.Series):
    def __init__(self, ddf, cols):
        cols = [Column(ddf, *col) if isinstance(col, tuple) else col for col in cols]
        super().__init__(cols, (col.name for col in cols))



class Column(object):
    def __init__(self, ddf, name, dtype):
        self.ddf = ddf
        self.name = name
        self.dtype = dtype



class DistributedDataFrame(Dataset):
    def __init__(self, src, index, columns):
        super().__init__(src.ctx, src)
        self.index = Columns(self, index)
        self.columns = Columns(self, columns)


    @classmethod
    def from_sample(cls, src, sample):
        index = sample.index
        if isinstance(index, pd.MultiIndex):
            index = [(level.name, level.dtype) for level in index.levels]
        else:
            index = [(index.name, index.dtype)]
        return cls(src, index, sample.dtypes.items())


    def parts(self):
        return [
            DistributedDataFramePartition(self, i, part)
            for i, part in enumerate(self.src.parts())
        ]


    def take(self, num):
        heads = self.map_partitions(lambda df: df.head(num)).icollect(eager=False, parts=True)
        try:
            df = next(heads)
            while len(df) < num:
                try:
                    df = df.append(next(heads))
                except StopIteration:
                    break
            if len(df) > num:
                df = df.head(num)
        finally:
            heads.close()
        return df


    def collect(self, parts=False):
        if parts:
            return super().collect(True)
        return super().glom().reduce(_pairwise_append)


class DistributedDataFramePartition(Partition):
    def _materialize(self, ctx):
        data = self.src.materialize(ctx)
        if isinstance(data, pd.DataFrame):
            data.__class__ = DataFrame
        return data
