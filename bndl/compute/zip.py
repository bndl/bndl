from bndl.compute.dataset import Dataset, Partition


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

    def _compute(self):
        return self.dset.comb(*(child.compute() for child in self.src))
