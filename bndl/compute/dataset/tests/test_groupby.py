from bndl.compute.dataset.tests import DatasetTest
from bndl.util.funcs import iseven


class GroupbyTest(DatasetTest):
    def test_groupby(self):
        self.assertEqual(self.ctx.range(10).group_by(iseven).map_values(sorted).collect(),
                         [(False, [1, 3, 5, 7, 9]), (True, [0, 2, 4, 6, 8])])

    def test_groupby_key(self):
        dset = self.ctx.range(10).key_by(iseven).group_by_key().map_values(sorted)
        self.assertEqual(dset.collect_as_map(), {
            False: [(False, i) for i in range(1, 10, 2)],
            True: [(True, i) for i in range(0, 10, 2)],
        })
