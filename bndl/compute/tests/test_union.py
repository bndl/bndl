from bndl.compute.tests import DatasetTest


class UnionTest(DatasetTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.a = cls.ctx.range(10)
        cls.b = cls.ctx.range(10, 20)
        cls.c = cls.ctx.range(20, 30)


    def test_union(self):
        self.assertEqual(sorted(self.a.union(self.b).collect()), sorted(list(range(10)) + list(range(10, 20))))


    def test_union_distict(self):
        self.assertEqual(sorted(self.a.union(self.b).distinct().collect()), list(range(10)) + list(range(10, 20)))


    def test_chains(self):
        aaa = self.a
        for _ in range(3):
            aaa = aaa.union(self.a)

        abc = self.a.union(self.b, self.c)
        cab = self.c.union(self.a.union(self.b))

        self.assertEqual(sorted(aaa.collect()), sorted(list(range(10)) * 4))
        self.assertEqual(sorted(abc.collect()), list(range(0, 30)))
        self.assertEqual(sorted(cab.collect()), list(range(0, 30)))


    def test_union_distinct_chains(self):
        chain = self.a.union(self.b).distinct().union(self.a).distinct().union(self.a).union(self.b)
        self.assertEqual(sorted(chain.collect()), sorted(list(range(20)) * 2))


    def test_self_union(self):
        self.assertEqual(sorted(self.a.union(self.a).collect()), sorted(list(range(10)) * 2))

        dset = self.a
        for _ in range(2):
            dset = dset.union(dset)
        self.assertEqual(sorted(dset.collect()), sorted(list(range(10)) * 4))
