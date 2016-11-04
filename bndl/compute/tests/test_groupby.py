from bndl.compute.tests import DatasetTest
from bndl.util.funcs import iseven


class GroupbyTest(DatasetTest):
    def test_group_by(self):
        self.assertEqual(self.ctx.range(10).group_by(iseven).map_values(sorted).collect(),
                         [(False, [1, 3, 5, 7, 9]), (True, [0, 2, 4, 6, 8])])
 
    def test_group_by_key(self):
        dset = self.ctx.range(100).key_by(iseven).group_by_key().map_values(sorted)
        self.assertEqual(dset.collect_as_map(), {
            False: list(range(1, 100, 2)),
            True: list(range(0, 100, 2)),
        })
