import unittest

from bndl.compute.run import create_ctx
from bndl.util.conf import Config


class ComputeTest(unittest.TestCase):
    worker_count = 3

    @classmethod
    def setUpClass(cls):
        config = Config()
        config['bndl.compute.worker_count'] = cls.worker_count
        config['bndl.net.listen_addresses'] = 'tcp://127.0.0.11:5000'
        cls.ctx = create_ctx(config, daemon=True)
        cls.ctx.await_workers(cls.worker_count)

    @classmethod
    def tearDownClass(cls):
        cls.ctx.stop()

DatasetTest = ComputeTest
