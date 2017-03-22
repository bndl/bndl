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

import numbers
import re
import string

import random as rand


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('(.)([0-9]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def random(length=64, alphabet=string.ascii_lowercase + string.digits, rng=None, seed=None):
    if rng:
        choice = rng.choice
    elif seed is None:
        choice = rand.choice
    else:
        choice = rand.Random(seed).choice
    return ''.join(choice(alphabet) for _ in range(length))


def decode(data, encoding='utf-8', errors='strict'):
    try:
        return data.decode(encoding, errors)
    except AttributeError as e:
        if isinstance(data, numbers.Number):
            raise
        try:
            return bytes(data).decode(encoding, errors)
        except:
            raise e
