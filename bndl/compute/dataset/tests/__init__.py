import unittest

from bndl.compute.driver import main


class DatasetTest(unittest.TestCase):
    worker_count = 4

    @classmethod
    def setUpClass(cls):
        cls.ctx = main(['--workers', str(cls.worker_count)])

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()
