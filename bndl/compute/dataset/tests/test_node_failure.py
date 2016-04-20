from bndl.compute.dataset.tests import DatasetTest


class NodeFailureTest(DatasetTest):
    # _transient_count = 3

    def setUp(self):
        super().setUp()
        self.dset = self.ctx.range(10)

    # @classmethod
    # def transient_failure(cls, *args, **kwargs):
    # return cls._transient_count
    #
    # def test_transient_failure(self):
    # # TODO
    # print(self.dset.map(NodeFailureTest.transient_failure).collect())

    def test_permanent_failure(self):
        with self.assertRaises(Exception):
            self.dset.map(lambda i: exec("raise ValueError('test')")).collect()
