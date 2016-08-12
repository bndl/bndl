import unittest

from bndl.util.threads import OnDemandThreadedExecutor


class OnDemandThreadedExecutorTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.executor = OnDemandThreadedExecutor()

    def test_submit(self):
        for v in range(10):
            result = self.executor.submit(lambda i: i, v).result()
            self.assertEqual(v, result)

    def test_map(self):
        iterable = range(10)
        result = self.executor.map(lambda i:i, iterable)
        self.assertEqual(list(iterable), list(result))
