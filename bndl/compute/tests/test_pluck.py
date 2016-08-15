
from bndl.compute.tests import DatasetTest


class PluckTest(DatasetTest):
    def test_pluck(self):
        dset = self.ctx.range(10, 100).map(str)
        self.assertEqual(''.join(dset.pluck(1).collect()), ''.join(map(str, range(10))) * 9)
