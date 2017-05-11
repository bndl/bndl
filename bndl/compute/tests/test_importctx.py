# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.case import TestCase

from bndl.util.strings import random_id


class TestCtxImport(TestCase):
    def test_ctximport(self):
        import bndl
        bndl.conf['bndl.net.cluster'] = random_id()
        bndl.conf['bndl.net.aio.uvloop'] = False
        bndl.conf['bndl.compute.executor_count'] = 3

        from bndl.compute import ctx
        executor_count = ctx.await_executors(connect_timeout=10, stable_timeout=10)
        self.assertTrue(executor_count > 0)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()

        self.assertEqual(ctx.await_executors(connect_timeout=10, stable_timeout=10), executor_count)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()


    def test_ctximport_noworkers(self):
        import bndl
        bndl.conf['bndl.net.cluster'] = random_id()
        bndl.conf['bndl.net.aio.uvloop'] = False
        bndl.conf['bndl.compute.executor_count'] = 0

        from bndl.compute import ctx
        with self.assertRaises(RuntimeError):
            ctx.await_executors(connect_timeout=3, stable_timeout=3)
