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

from math import factorial
import sys

from bndl.compute.tests import DatasetTest


class AccumulatorTest(DatasetTest):
    def test_ops(self):
        inc_accum = self.ctx.accumulator(0)
        dec_accum = self.ctx.accumulator(0)
        mul_accum = self.ctx.accumulator(1)
        div_accum = self.ctx.accumulator(1)
        lshift_accum = self.ctx.accumulator(1)
        rshift_accum = self.ctx.accumulator(sys.maxsize)
        and_accum = self.ctx.accumulator(15)
        or_accum = self.ctx.accumulator(0)

        def update(i):
            nonlocal inc_accum, dec_accum, mul_accum, div_accum, \
                     lshift_accum, rshift_accum, and_accum, or_accum
            inc_accum += i
            dec_accum -= i
            mul_accum *= i + 1
            div_accum /= i + 1
            lshift_accum <<= i
            rshift_accum >>= i
            and_accum &= i
            or_accum |= i
            return i

        r = range(10)
        c = self.ctx.collection(r).map(update).count()
        self.assertEqual(c, len(r))
        self.assertEqual(inc_accum.value, sum(r))
        self.assertEqual(dec_accum.value, -sum(r))
        self.assertEqual(mul_accum.value, factorial(r.stop))
        self.assertAlmostEqual(div_accum.value, 1 / factorial(r.stop))
        self.assertEqual(lshift_accum.value, 1 << sum(r))
        self.assertEqual(rshift_accum.value, sys.maxsize >> sum(r))
        self.assertEqual(and_accum.value, 0)
        self.assertEqual(or_accum.value, 15)


    def test_cancelling_ops(self):
        inc_accum = self.ctx.accumulator(0)
        incdec_accum = self.ctx.accumulator(0)
        muldiv_accum = self.ctx.accumulator(1)
        shift_accum = self.ctx.accumulator(sys.maxsize // 2)

        def update(i):
            nonlocal inc_accum, incdec_accum, muldiv_accum, shift_accum
            inc_accum += i
            incdec_accum += i
            incdec_accum -= i
            muldiv_accum *= i + 1
            muldiv_accum /= i + 1
            shift_accum <<= i
            shift_accum >>= i

        r = range(10)
        self.ctx.collection(r).map(update).execute()
        self.assertEqual(inc_accum.value, 45)
        self.assertEqual(incdec_accum.value, 0)
        self.assertAlmostEqual(muldiv_accum.value, 1)
        self.assertEqual(shift_accum.value, sys.maxsize // 2)


    def test_set(self):
        accum = self.ctx.accumulator(set())

        def update(i):
            accum.update('add', i)

        self.ctx.range(10).map(update).execute()

        self.assertEqual(accum.value, set(range(10)))
