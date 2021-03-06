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


class RangeDataset(Dataset):
    def __init__(self, ctx, start, stop=None, step=1, pcount=None, dset_id=None):
        # TODO test / fix negative step
        super().__init__(ctx, dset_id=dset_id)

        if not stop:
            stop = start
            start = 0

        if pcount is None:
            pcount = ctx.default_pcount

        self.start = start
        self.stop = stop
        self.step = step
        self.pcount = pcount


    def parts(self):
        len_ = len(range(self.start, self.stop, self.step))
        return [
            RangePartition(self, idx, range(
                self.start + idx * len_ // self.pcount * self.step,
                self.start + (idx + 1) * len_ // self.pcount * self.step,
                self.step
            ))
            for idx in range(self.pcount)
        ]


    def __str__(self):
        if self.start == 0:
            s = str(self.stop)
        else:
            s = '%s,%s' % (self.start, self.stop)

        if self.step != 1:
            s += ',' + str(self.step)

        return 'range(' + s + ')'



class RangePartition(Partition):
    def __init__(self, dset, idx, subrange):
        super().__init__(dset, idx)
        self.subrange = subrange


    def _compute(self):
        return self.subrange
