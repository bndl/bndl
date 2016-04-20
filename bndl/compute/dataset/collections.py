from collections.abc import Sized
from math import ceil

from bndl.compute.dataset.base import Dataset, IterablePartition
from bndl.util.collection import batch


class DistributedCollection(Dataset):
    def __init__(self, ctx, c, pcount=None, psize=None):
        super().__init__(ctx)
        self._c = c

        if pcount is not None and pcount <= 0:
            raise ValueError('pcount must be None or > 0')
        if psize is not None and psize <= 0:
            raise ValueError('psize must be None or > 0')
        if pcount and psize:
            raise ValueError("can't set both pcount and psize")
        elif ctx.default_pcount <= 0:
            raise Exception("can't use default_pcount, no workers available")

        if psize:
            self.pcount = None
            self.psize = psize
        else:
            self.pcount = pcount or ctx.default_pcount
            self.psize = None


    def parts(self):
        if self.psize:
            parts = [
                IterablePartition(self, i, list(b))
                for i, b in enumerate(batch(self._c, self.psize))
            ]
            self.pcount = parts
            return parts
        else:
            c = self._c
            if not isinstance(c, Sized):
                c = list(c)
            step = max(1, ceil(len(c) / self.pcount))
            slices = (
                (idx, c[idx * step: (idx + 1) * step])
                for idx in range(self.pcount)
            )
            return [
                IterablePartition(self, idx, slice)
                for idx, slice in slices  # @ReservedAssignment
                if len(slice)
            ]

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_c']
        return state
