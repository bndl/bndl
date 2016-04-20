from itertools import chain
import string

from bndl.compute.dataset.tests import DatasetTest
from bndl.util.funcs import iseven, neg, identity, isodd


class RangeTest(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(10, pcount=3)

    def test_key_by(self):
        is_even = self.dset.key_by(iseven)
        self.assertEqual(is_even.count(), 10)
        self.assertEqual(is_even.collect(), [(True, 0), (False, 1), (True, 2), (False, 3), (True, 4), (False, 5), (True, 6), (False, 7), (True, 8), (False, 9)])
        self.assertEqual(is_even.keys().collect(), [True, False] * 5)
        self.assertEqual(is_even.values().collect(), list(range(10)))

    def test_values_as(self):
        self.assertEqual(self.ctx.range(10).values_as(neg).collect(), list(zip(range(10), range(0, -10, -1))))

    def test_mapping(self):
        is_even = self.dset.key_by(iseven)
        self.assertEqual(is_even.map_keys(lambda k: str(k)).keys().collect(), ['True', 'False'] * 5)
        self.assertEqual(is_even.map_values(iseven).collect(), [(True, True), (False, False)] * 5)

    def test_flatmap_values(self):
        self.assertEqual(
            self.ctx.range(10).values_as(lambda i: string.ascii_lowercase[i] * (i + 1)).flatmap_values(identity).collect(),
            list(chain.from_iterable(string.ascii_lowercase[i] * (i + 1) for i in range(10)))
        )

    def test_collect_as_map(self):
        self.assertEqual(self.dset.key_by(identity).collect_as_map(), {i:i for i in range(10)})
        self.assertEqual(
            self.dset.group_by(iseven).collect_as_map(), {
                True: list(filter(iseven, range(0, 10))),
                False: list(filter(isodd, range(0, 10))),
            }
        )
