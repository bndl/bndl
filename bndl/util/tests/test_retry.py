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

import time
import unittest

from bndl.util.retry import do_with_retry


class RetryTestCase(unittest.TestCase):
    def test_succes(self):
        retry_backoff = 1.05
        retry_limits = (0, 1, 2, 3)

        start = time.time()

        for retry_limit in retry_limits:
            attempt = 0
            def action():
                nonlocal attempt
                if attempt < retry_limit:
                    attempt += 1
                    raise Exception('fail')

            do_with_retry(action, retry_limit, retry_backoff)
            self.assertEqual(attempt, retry_limit)

        end = time.time()
        expected_dur = sum(
            retry_backoff ** attempt - 1
            for retry_limit in retry_limits
            for attempt in range(retry_limit + 1)
        )
        self.assertAlmostEqual(end - start, expected_dur, delta=0.1)


    def test_fail(self):
        retry_limits = (0, 1, 2, 3)

        for retry_limit in retry_limits:
            attempt = 0
            def action():
                nonlocal attempt
                attempt += 1
                raise ValueError('fail')

            with self.assertRaisesRegex(ValueError, 'fail'):
                do_with_retry(action, retry_limit)
            self.assertEqual(attempt, retry_limit + 1)


    def test_fail_nontransient(self):
        attempt = 0
        def action():
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise ValueError('fail')
            elif attempt == 2:
                raise KeyError('fail')

        attempt = 0
        do_with_retry(action, 2)

        attempt = 0
        do_with_retry(action, 2, transients=(ValueError, KeyError,))

        attempt = 0
        with self.assertRaisesRegex(KeyError, 'fail'):
            do_with_retry(action, 2, transients=(ValueError,))


    def test_noretry(self):
        attempt = 0
        def action():
            nonlocal attempt
            attempt += 1
            raise Exception('fail')

        for retry_limit in (0, None):
            attempt = 0
            with self.assertRaisesRegex(Exception, 'fail'):
                do_with_retry(action, retry_limit)
                self.assertEqual(attempt, 1)
