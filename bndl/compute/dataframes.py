from bndl.compute.dataset import Dataset
import pandas as pd


_pairwise_append = lambda a, b:a.append(b)


class DataFrame(pd.DataFrame):
    def __iter__(self):
        return self.itertuples(name='Row')


class DistributedDataFrame(Dataset):
    def __init__(self, src, index, columns, dtypes):
        super().__init__(src.ctx, src)
        self.index = index
        self.columns = columns
        self.dtypes = dtypes


    def parts(self):
        return self.src.parts()


    def take(self, num):
        heads = self.map_partitions(lambda df: df.head(num)).icollect(eager=False, parts=True)
        try:
            df = next(heads)
            while len(df) < num:
                try:
                    df.append(next(heads))
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
