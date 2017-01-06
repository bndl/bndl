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

from bndl.compute.dataset import Dataset
import scipy.sparse


class DistributedSparseMatrix(Dataset):
    '''
    Distributed data set with scipy sparce matrices as partitions. Currently
    assumes the matrix is split row wise. The take and collect operators
    therefore stack the matrix partitions vertically.
    '''
    def __init__(self, src):
        super().__init__(src.ctx, src)


    def parts(self):
        return self.src.parts()


    def take(self, num):
        matrices = self.map_partitions(lambda matrix: matrix[:min(num, matrix.shape[0])])._itake_parts()
        try:
            rows = 0
            blocks = []
            while rows < num:
                try:
                    matrix = next(matrices)
                except StopIteration:
                    break
                if rows + matrix.shape[0] > num:
                    matrix = matrix[:num - rows]
                blocks.append(matrix)
                rows += matrix.shape[0]
                if rows >= num:
                    break
        finally:
            matrices.close()
        return scipy.sparse.vstack(blocks)


    def collect(self, parts=False):
        matrices = super().collect(True)
        if parts:
            return matrices
        return scipy.sparse.vstack(matrices)
