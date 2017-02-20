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

from contextlib import contextmanager
import logging


logger = logging.getLogger(__name__)


@contextmanager
def catch(*ignore, log_level=logging.DEBUG):
    try:
        yield
    except Exception as exc:
        if ignore and not any(isinstance(exc, i) for i in ignore):
            raise exc
        else:
            logger.log(log_level, 'An exception occurred: %s' % type(exc), exc_info=True)
