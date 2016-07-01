from unittest.case import TestCase
from bndl.util.objects import LazyObject
import pickle
from collections import Counter
from functools import partial

class Expensive(object):
    def __init__(self, counters):
        self.counters = counters
        self.counters['created'] += 1
        self.x = '1'

    def stop(self):
        self.counters['destroyed'] += 1

    def __eq__(self, other):
        return isinstance(other, Expensive) and self.x == other.x


class TestLazyObject(TestCase):
    def setUp(self):
        self.counters = Counter()
        self.l = LazyObject(partial(Expensive, self.counters), destructor='stop')


    def test_factory(self):
        self.assertEqual(self.l.x, '1')


    def test_destructor(self):
        self.assertEqual(self.counters['created'], 0)
        self.assertEqual(self.counters['destroyed'], 0)

        self.assertEqual(self.l.x, '1')
        self.assertEqual(self.counters['created'], 1)
        self.assertEqual(self.counters['destroyed'], 0)

        self.l.stop()
        self.assertEqual(self.counters['created'], 1)
        self.assertEqual(self.counters['destroyed'], 1)

        self.assertEqual(self.l.x, '1')
        self.assertEqual(self.counters['created'], 2)
        self.assertEqual(self.counters['destroyed'], 1)


    def test_pickle(self):
        p = pickle.dumps(self.l)
        l2 = pickle.loads(p)
        self.assertEqual(self.l, l2)
