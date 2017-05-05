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
import io

from bndl.compute.tests import ComputeTest
from itertools import takewhile


class ProfilingTest(ComputeTest):
    def test_cpu_profiling(self):
        prof = self.ctx.cpu_profiling

        prof.start()
        self.ctx.range(100).mean()
        prof.stop()
        stats1 = prof.get_stats()
        
        self.ctx.range(100).mean()
        stats2 = prof.get_stats()

        self.assertEqual(stats1, stats2)

        out = io.StringIO()
        prof.print_stats(10, file=out, include='bndl')
        
        # strip out the timing lines
        # strip the header
        lines = iter(out.getvalue().splitlines())
        for l in lines:
            if l.startswith('name   '):
                break
        lines = list(lines)
        # strip the footer 
        start = lines[0].split('/', 1)[0] + '/'
        lines = list(takewhile(lambda l: l.startswith(start), lines))
        
        # check if there are 10 lines
        self.assertEqual(len(lines), 10)
