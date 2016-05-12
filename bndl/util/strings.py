import string

import random as rand


def random(length=64, alphabet=string.ascii_lowercase + string.digits, rng=None, seed=None):
    if not rng:
        rng = rand.Random(seed)
    return ''.join(rng.choice(alphabet) for _ in range(length))
