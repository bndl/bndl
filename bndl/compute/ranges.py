from bndl.compute.dataset import Dataset, IterablePartition


class RangeDataset(Dataset):
    def __init__(self, ctx, start, stop=None, step=1, pcount=None):
        # TODO test / fix negative step
        super().__init__(ctx)
        if not stop:
            stop = start
            start = 0
        self.start = start
        self.stop = stop
        self.step = step
        self.len = len(range(start, stop, step))
        self.pcount = pcount or ctx.default_pcount


    def parts(self):
        parts = (
            IterablePartition(self, idx, range(
                self._subrange_start(idx),
                self._subrange_start(idx + 1),
                self.step
            ))
            for idx in range(self.pcount)
        )
        return [part for part in parts if part.iterable]


    def _subrange_start(self, idx):
        return self.start + idx * self.len // self.pcount * self.step


    def __str__(self):
        if self.start == 0:
            s = str(self.stop)
        else:
            s = ','.join((self.start, self.stop))

        if self.step != 1:
            s += ',' + str(self.step)

        return 'range(' + s + ')'
