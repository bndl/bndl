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

import logging.config
import os.path

from bndl.util.conf import Config
from bndl.util.objects import LazyObject
from bndl.util.log import install_trace_logging


# Expose a global BNDL configuration

conf = LazyObject(Config)


# Configure Logging

install_trace_logging()

if os.path.exists('logging.conf'):
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)


# BNDL version info

__version_info__ = (0, 6, 0, 'dev1')
__version__ = '.'.join(map(str, __version_info__))
