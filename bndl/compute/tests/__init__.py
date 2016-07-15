import unittest

from bndl.compute import create_ctx
from bndl.util.conf import Config


class ComputeTest(unittest.TestCase):
    worker_count = 4

    @classmethod
    def setUpClass(cls):
        config = Config()
        config['bndl.compute.workers'] = cls.worker_count
        cls.ctx = create_ctx(config, daemon=True)
        if cls.ctx.worker_count < cls.worker_count:
            cls.ctx.await_workers()

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()
