# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
