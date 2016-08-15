from itertools import product

from bndl.compute.tests import DatasetTest
from bndl.util.funcs import iseven, isodd


class JoinTest(DatasetTest):
    def test_join(self):
        expected = [
            (False, list(product(list(filter(isodd, range(0, 5))), list(filter(isodd, range(5, 10)))))),
            (True, list(product(list(filter(iseven, range(0, 5))), list(filter(iseven, range(5, 10)))))),
        ]
        a = self.ctx.range(0, 5).key_by(iseven)
        b = self.ctx.range(5, 10).key_by(iseven)
        self.assertEqual(a.join(b).map_values(sorted).collect(), expected)


    def test_join_chains(self):
        a = self.ctx.range(100).key_by(lambda i: str(i // 2))
        b = self.ctx.range(100).key_by(lambda i: str(i // 2))
        c = self.ctx.range(100).key_by(lambda i: str(i // 2))

        ab = a.join(b)
        cab = c.join(ab)
        abc = ab.join(c)

        self.assertEqual(cab.count(), 50)
        self.assertEqual(abc.count(), 50)
        self.assertEqual(cab.keys().collect_as_set(), abc.keys().collect_as_set())


    def test_join_on(self):
        expected = [
            list(product(list(filter(isodd, range(0, 5))), list(filter(isodd, range(5, 10))))),
            list(product(list(filter(iseven, range(0, 5))), list(filter(iseven, range(5, 10))))),
        ]
        a = self.ctx.range(0, 5)
        b = self.ctx.range(5, 10)
        self.assertEqual(a.join(b, iseven).keys().collect_as_set(), {False, True})
        self.assertEqual(a.join(b, iseven).values().map(sorted).collect(), expected)
