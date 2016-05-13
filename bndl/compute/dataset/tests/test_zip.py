from bndl.compute.dataset.tests import DatasetTest


class ZipTest(DatasetTest):

    def test_zip(self):
        a = self.ctx.range(1000)
        b = a.map(str)
        self.assertEqual(a.zip(b).collect(),
                         list(zip(range(1000), map(str, range(1000)))))
