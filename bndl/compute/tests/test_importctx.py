from unittest.case import TestCase


class TestCtxImport(TestCase):
    def test_ctximport(self):
        from bndl.compute import ctx

        self.assertTrue(ctx.worker_count > 0)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()

        self.assertTrue(ctx.worker_count > 0)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()
