from bndl.compute.tests import DatasetTest


class ZipTest(DatasetTest):

    def test_zip(self):
        left = self.ctx.range(1000)
        right = left.map(str)
        self.assertEqual(left.zip(right).collect(),
                         list(zip(range(1000), map(str, range(1000)))))
