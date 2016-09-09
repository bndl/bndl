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
        matrices = self.map_partitions(lambda matrix: matrix[:min(num, matrix.shape[0])]).icollect(eager=False, parts=True)
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
