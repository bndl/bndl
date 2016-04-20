from bndl.compute.dataset.tests import DatasetTest


class UnionTest(DatasetTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.a = cls.ctx.range(10)
        cls.b = cls.ctx.range(10, 20)
        cls.union = cls.a.union(cls.b)

    def test_union(self):
        self.assertEqual(sorted(self.union.collect()), sorted(list(range(10)) + list(range(10, 20))))

    def test_union_distict(self):
        self.assertEqual(sorted(self.union.distinct().collect()), list(range(10)) + list(range(10, 20)))

    def test_chains(self):
        x = self.a
        for _ in range(3):
            x = x.union(self.a)
        self.assertEqual(sorted(x.collect()) , sorted(list(range(10)) * 4))

    def test_union_distinct_chains(self):
        chain = self.union.distinct().union(self.a).distinct().union(self.a).union(self.b)
        self.assertEqual(sorted(chain.collect()), sorted(list(range(20)) * 2))
        # self.assertEqual(sorted(chain.distinct().collect()), sorted(list(range(20))))

    def test_self_union(self):
        x = self.a
        for _ in range(2):
            x = x.union(x)
        self.assertEqual(sorted(x.collect()), sorted(list(range(10)) * 4))

