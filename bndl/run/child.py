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

import importlib
import signal
import sys


def exit_handler(sig, frame):
    assert sig != signal.SIGINT
    sys.exit(sig)


def main():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, exit_handler)
    script, module, main, *args = sys.argv
    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()


if __name__ == '__main__':
    main()
