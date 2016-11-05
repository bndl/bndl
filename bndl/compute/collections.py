from collections import Sized
from math import ceil

from bndl.compute.dataset import Dataset, IterablePartition
from bndl.util.collection import batch


class DistributedCollection(Dataset):
    def __init__(self, ctx, collection, pcount=None, psize=None):
        super().__init__(ctx)
        self.collection = collection

        if pcount is not None and pcount <= 0:
            raise ValueError('pcount must be None or > 0')
        if psize is not None and psize <= 0:
            raise ValueError('psize must be None or > 0')
        if pcount and psize:
            raise ValueError("can't set both pcount and psize")

        if psize:
            self.pcount = None
            self.psize = psize
        else:
            self.pcount = pcount
            self.psize = None


    def parts(self):
        if self.psize:
            parts = [
                IterablePartition(self, i, list(b))
                for i, b in enumerate(batch(self.collection, self.psize))
            ]
            self.pcount = len(parts)
            return parts
        else:
            if not self.pcount:
                pcount = self.ctx.default_pcount
                if pcount <= 0:
                    raise Exception("can't use default_pcount, no workers available")
            else:
                pcount = self.pcount

            if not isinstance(self.collection, Sized):
                self.collection = list(self.collection)

            step = max(1, ceil(len(self.collection) / pcount))
            slices = (
                (idx, self.collection[idx * step: (idx + 1) * step])
                for idx in range(pcount)
            )

            return [
                IterablePartition(self, idx, slice)
                for idx, slice in slices  # @ReservedAssignment
                if len(slice)
            ]


    def __getstate__(self):
        state = super().__getstate__()
        del state['collection']
        return state
