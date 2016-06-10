from functools import reduce
from itertools import chain

from bndl.compute.dataset.base import Dataset, Partition


class ZippedDataset(Dataset):
    def __init__(self, *sources, comb, dset_id=None):
        assert len(sources) > 1
        self.srcparts = [src.parts() for src in sources]
        self.pcount = len(self.srcparts[0])
        assert all(self.pcount == len(srcparts) for srcparts in self.srcparts[1:])
        self.comb = comb
        super().__init__(sources[0].ctx, src=sources, dset_id=dset_id)

    def parts(self):
        return [ZippedPartition(self, i, [srcpart[i] for srcpart in self.srcparts])
                for i in range(self.pcount)]


class ZippedPartition(Partition):
    def __init__(self, dset, idx, children):
        super().__init__(dset, idx, children)

    def _preferred_workers(self, workers):
        prefs = [set(child.preferred_workers(workers) or ()) for child in self.src]
        matches = reduce(lambda a, b: a.intersection(b), prefs)
        if matches:
            return matches
        else:
            return set(chain.from_iterable(prefs))

    def _materialize(self, ctx):
        yield from self.dset.comb(*(child.materialize(ctx) for child in self.src))
