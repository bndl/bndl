from bndl.compute.dataset.tests import DatasetTest
from bndl.util.funcs import iseven


class GroupbyTest(DatasetTest):
    def test_groupby(self):
        self.assertEqual(self.ctx.range(10).group_by(iseven).collect(), [(False, [1, 3, 5, 7, 9]), (True, [0, 2, 4, 6, 8])])
