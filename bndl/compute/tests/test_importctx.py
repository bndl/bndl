from unittest.case import TestCase


class TestCtxImport(TestCase):
    def test_ctximport(self):
        from bndl.compute import ctx

        worker_count = ctx.await_workers()
        self.assertTrue(worker_count > 0)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()

        self.assertEqual(ctx.await_workers(), worker_count)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()
