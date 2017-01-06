# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from itertools import product, chain, groupby

from bndl.compute.tests import DatasetTest
from bndl.util.funcs import iseven, isodd
from operator import itemgetter


def groups(partition):
    for key, group in groupby(partition, key=itemgetter(0)):
        yield key, list(group)


class JoinTest(DatasetTest):
    def test_join(self):
        a = range(0, 50)
        b = range(50, 100)

        expected = [
            (False, list(product(filter(isodd, a), filter(isodd, b)))),
            (True, list(product(filter(iseven, a), filter(iseven, b)))),
        ]

        a = self.ctx.collection(a, pcount=3).key_by(iseven)
        b = self.ctx.collection(b, pcount=3).key_by(iseven)
        ab = a.join(b, pcount=4)

        self.assertEqual(ab.map_values(sorted).collect(), expected)


    def test_join_chains(self):
        a = self.ctx.range(100, pcount=3).key_by(lambda i: str(i // 2)).cache()
        b = a
        c = a

        ab = a.join(b, pcount=3)
        cab = c.join(ab, pcount=3)
        abc = ab.join(c, pcount=3)

        self.assertEqual(cab.count(), 50)
        self.assertEqual(abc.count(), 50)
        self.assertEqual(cab.keys().collect_as_set(), abc.keys().collect_as_set())


    def test_join_on(self):
        a = range(0, 5)
        b = range(5, 10)

        expected = [
            list(product(filter(isodd, a), filter(isodd, b))),
            list(product(filter(iseven, a), filter(iseven, b))),
        ]

        a = self.ctx.collection(a).cache()
        b = self.ctx.collection(b).cache()

        self.assertEqual(a.join(b, key=iseven).keys().collect_as_set(), {False, True})
        self.assertEqual(a.join(b, key=iseven).values().map(sorted).collect(), expected)


    def test_cogroup(self):
        a = self.ctx.range(0, 10).key_by(lambda i: str(i % 4)).cache()
        b = self.ctx.range(10, 20).key_by(lambda i: str(i % 4)).cache()
        c = self.ctx.range(20, 30).key_by(lambda i: str(i % 4)).cache()

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
