from bndl.compute.dataset.tests import DatasetTest


class ManyNodesTest(DatasetTest):
    worker_count = 8

    def test_many_nodes(self):
        self.assertEqual(self.ctx.range(1, 100).mean(), 50)
