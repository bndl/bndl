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

from itertools import islice


cpdef bint marshalable(object obj):
    if obj is None:
        return True

    t = type(obj)

    if t in (bool, int, float, complex, str, bytes, bytearray):
        return True

    elif t in (tuple, list, set, frozenset):
        for e in islice(obj, 100):
            if not marshalable(e):
                return False
        return True

    elif t == dict:
        for k, v in islice(obj.items(), 100):
            if not marshalable(k) or not marshalable(v):
                return False
        return True

    else:
        return False
