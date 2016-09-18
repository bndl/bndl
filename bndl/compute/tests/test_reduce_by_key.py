from collections import Counter
from operator import add

from bndl.compute.tests import DatasetTest
from bndl.util import strings


class ReduceByKeyTest(DatasetTest):
    def test_wordcount(self):
        words = [strings.random(2) for _ in range(100)] * 5
        counts = Counter(words)
        dset = self.ctx.collection(words, pcount=4).with_value(1).reduce_by_key(add)
        self.assertEqual(dset.count(), len(counts))
        for word, count in dset.collect():
            self.assertTrue(word in counts)
            self.assertEqual(count, counts[word])
