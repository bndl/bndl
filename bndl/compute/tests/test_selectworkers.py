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

from bndl.compute.tests import DatasetTest
from bndl.execute.worker import current_worker
from collections import Counter


class SelectWorkersTest(DatasetTest):
    def test_require(self):
        executed_on = self.ctx.accumulator(Counter())
        def register_worker(i):
            nonlocal executed_on
            executed_on += Counter({current_worker().name:1})

        dset = self.ctx.range(10).map(register_worker)

        targeted = Counter()
        for _ in range(2):
            for worker in self.ctx.workers:
                worker_name = worker.name
                targeted[worker_name] += 10
                dset.require_workers(lambda workers: [w for w in workers if w.name == worker_name]).execute()
                self.assertEqual(executed_on.value, targeted)
