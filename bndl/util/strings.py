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
