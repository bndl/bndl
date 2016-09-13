from itertools import product, chain

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
        self.assertEqual(a.join(b, key=iseven).keys().collect_as_set(), {False, True})
        self.assertEqual(a.join(b, key=iseven).values().map(sorted).collect(), expected)


    def test_cogroup(self):
        a = self.ctx.range(0, 10).key_by(lambda i: str(i % 4))
        b = self.ctx.range(10, 20).key_by(lambda i: str(i % 4))
        c = self.ctx.range(20, 30).key_by(lambda i: str(i % 4))

        abc = a.cogroup(b, c)
        self.assertEqual(abc.count(), 4)
        self.assertEqual(sorted(abc.keys().collect()), list('0123'))
        self.assertEqual(sorted(abc.values().flatmap().flatmap().collect()),
                         sorted(
                            a.values().collect() +
                            b.values().collect() +
                            c.values().collect()
                        ))
        self.assertTrue(all(abc.starmap(lambda key, groups: list(str(v % 4) == key for v in chain(*groups))).flatmap().collect()))
