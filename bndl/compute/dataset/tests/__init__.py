import sys
import unittest
from bndl.util.log import configure_console_logging
from bndl.compute.driver import main
import time


class DatasetTest(unittest.TestCase):
    worker_count = 4

    @classmethod
    def setUpClass(cls):
        cls.ctx = main(['--workers', str(cls.worker_count)])

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()
