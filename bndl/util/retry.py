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


def retry_delay(timeout_backoff, retry_round):
    assert timeout_backoff > 1
    assert retry_round >= 1
    return timeout_backoff ** retry_round - 1


def do_with_retry(action, limit=1, backoff=None, transients=(Exception,)):
    assert not backoff or backoff > 1
    assert not limit or limit >= 0

    fails = 0

    while True:
        try:
            return action()
        except transients:
            fails += 1
            if not limit or fails > limit:
                raise
            elif backoff:
                sleep = retry_delay(backoff, fails)
                time.sleep(sleep)
