import re
import string

import random as rand


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('(.)([0-9]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def random(length=64, alphabet=string.ascii_lowercase + string.digits, rng=None, seed=None):
    if not rng:
        rng = rand.Random(seed)
    return ''.join(rng.choice(alphabet) for _ in range(length))
