from bndl.compute.tests import DatasetTest


class FilterTest(DatasetTest):
    def test_filter_lamba(self):
        self.assertEqual(self.ctx.range(1, 100).filter(lambda i: i % 2).count(), 50)

    def test_filter_bool(self):
        self.assertEqual(self.ctx.range(1, 100).map(lambda i: i % 2).filter().count(), 50)

    def test_starfilter(self):
        self.assertEqual(self.ctx.range(1, 100).key_by_id().starfilter(lambda a, b: b >= 50).count(), 50)
