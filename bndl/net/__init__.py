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

import uuid

from bndl.net.connection import getlocalhostname
from bndl.util.conf import CSV, String


listen_addresses = CSV(desc='The addresses for the local BNDL node to listen on.')
listen_addresses.default = ['tcp://%s:5000' % getlocalhostname()]

seeds = CSV(desc='The seed addresses for BNDL nodes to form a cluster through gossip.')

machine = String(str(uuid.getnode()), desc='The machine identifier of the BNDL node')
cluster = String('default', desc='The name of the BNDL cluster, nodes in another cluster are ignored.')
