import unittest

from bndl.compute.run import create_ctx
from bndl.util.conf import Config


class ComputeTest(unittest.TestCase):
    worker_count = 4

    @classmethod
    def setUpClass(cls):
        config = Config()
        config['bndl.compute.workers'] = cls.worker_count
        cls.ctx = create_ctx(config, daemon=True)
        cls.ctx.await_workers(cls.worker_count)

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()
