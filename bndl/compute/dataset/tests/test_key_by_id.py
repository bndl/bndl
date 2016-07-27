from bndl.compute.dataset.tests import DatasetTest


class KeyByIdTest(DatasetTest):
    def test_key_by_id(self):
        with_id = self.ctx.range(10000).key_by_id()
        self.assertEqual(with_id.count(), with_id.keys().count_distinct())

    def test_key_by_idx(self):
        with_idx = self.ctx.range(10000).key_by_idx()
        self.assertEqual(with_idx.keys().collect(), with_idx.values().collect())
