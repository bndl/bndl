import random
import re
import string


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('(.)([0-9]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def random_string(size=8, chars=string.ascii_uppercase + string.digits, choice=random.choice):
    return ''.join(choice(chars) for _ in range(size))
