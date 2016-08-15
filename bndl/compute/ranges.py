from bndl.compute.dataset import Dataset, IterablePartition


class DistributedRange(Dataset):
    def __init__(self, ctx, start, stop=None, step=1, pcount=None):
        # TODO test / fix negative step
        super().__init__(ctx)
        if not stop:
            stop = start
            start = 0
        self.range = range(start, stop, step)
        self.pcount = pcount or ctx.default_pcount

    def parts(self):
        parts = (
            IterablePartition(self, idx, range(
                self._subrange_start(idx),
                self._subrange_start(idx + 1),
                self.range.step
            ))
            for idx in range(self.pcount)
        )
        return [part for part in parts if part.iterable]

    def _subrange_start(self, idx):
        return self.range.start + idx * len(self.range) // self.pcount * self.range.step
