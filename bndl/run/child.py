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
import os
import signal
import sys

from bndl.rmi.node import RMINode


control_node = None


def exit_handler(sig, frame):
    sys.exit(sig)


def main():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, exit_handler)

    script, module, main, supervisor_address, *args = sys.argv

    global control_node
    control_node = RMINode(name='supervisor.child.%s' % os.getpid(),
                       addresses=['127.0.0.1:0'],
                       cluster=None,
                       seeds=[supervisor_address])
    control_node.start_async().result()

    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()


if __name__ == '__main__':
    main()
