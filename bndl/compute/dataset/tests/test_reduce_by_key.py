from bndl.compute.dataset.tests import DatasetTest
from operator import add
from bndl.util import strings
import re
from collections import Counter


class ReduceByKeyTest(DatasetTest):
    def test_wordcount(self):
        words = [strings.random(2) for _ in range(100)] * 5
        counts = Counter(words)
        dset = self.ctx.collection(words, pcount=4).with_value(1).reduce_by_key(add)
        self.assertEqual(dset.count(), len(counts))
        for w, c in dset.collect():
            self.assertTrue(w in counts)
            self.assertEqual(c, counts[w])
