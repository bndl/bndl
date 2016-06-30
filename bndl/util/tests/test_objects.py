from unittest.case import TestCase
from bndl.util.objects import LazyObject


class TestLazyObject(TestCase):
    def test_lazyobject(self):
        TestLazyObject.test_lazyobject.created = False
        class Expensive(object):
            def __init__(self):
                TestLazyObject.test_lazyobject.created = True
                self.x = '1'

        def factory():
            return Expensive()

        l = LazyObject(factory)
        self.assertEqual(TestLazyObject.test_lazyobject.created, False)
        self.assertEqual(l.x, '1')
        self.assertEqual(TestLazyObject.test_lazyobject.created, True)
