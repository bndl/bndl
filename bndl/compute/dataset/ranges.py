from bndl.compute.dataset.base import Dataset, IterablePartition


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
        return [
            part for part in
            (IterablePartition(self, idx, range(
                self._subrange_start(idx),
                self._subrange_start(idx + 1),
                self.range.step
            ))
            for idx in range(self.pcount))
            if part.iterable
        ]

    def _subrange_start(self, idx):
        r = self.range
        return r.start + idx * len(r) // self.pcount * r.step
