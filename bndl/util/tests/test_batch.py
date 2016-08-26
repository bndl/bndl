from unittest.case import TestCase
from bndl.util.collection import batch
class BatchingTest(TestCase):
    def test_batch_with_step(self):
        res = []
        for b in batch(list(range(10)), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_range(self):
        res = []
        for b in batch(range(10), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_iterator(self):
        res = []
        for b in batch(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))

    def test_generator(self):
        res = []
        for b in batch((i for i in range(10)), 3):
            self.assertTrue(len(b) <= 3)
            res.extend(b)
        self.assertEqual(res, list(range(10)))
