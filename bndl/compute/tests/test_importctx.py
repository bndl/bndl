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


class TestCtxImport(TestCase):
    def test_ctximport(self):
        from bndl.compute import ctx

        worker_count = ctx.await_workers(connect_timeout=120, stable_timeout=120)
        self.assertTrue(worker_count > 0)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()

        self.assertEqual(ctx.await_workers(connect_timeout=120, stable_timeout=120), worker_count)
        self.assertEqual(ctx.range(100).count(), 100)
        ctx.stop()
