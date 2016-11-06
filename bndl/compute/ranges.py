from bndl.compute.dataset import Dataset, IterablePartition


class RangeDataset(Dataset):
    def __init__(self, ctx, start, stop=None, step=1, pcount=None, dset_id=None):
        # TODO test / fix negative step
        super().__init__(ctx, dset_id=dset_id)

        if not stop:
            stop = start
            start = 0

        if pcount is None:
            pcount = ctx.default_pcount

        len_ = len(range(start, stop, step))
        parts = (
            IterablePartition(self, idx, range(
                start + idx * len_ // pcount * step,
                start + (idx + 1) * len_ // pcount * step,
                step
            ))
            for idx in range(pcount)
        )

        self._parts = [part for part in parts if part.iterable]

        self.start = start
        self.stop = stop
        self.step = step
        self.pcount = pcount


    def parts(self):
        return self._parts


    def __str__(self):
        if self.start == 0:
            s = str(self.stop)
        else:
            s = ','.join((self.start, self.stop))

        if self.step != 1:
            s += ',' + str(self.step)

        return 'range(' + s + ')'


    def __reduce__(self):
        return RangeDataset, (self.ctx, self.start, self.stop, self.step, self.pcount, self.id)
