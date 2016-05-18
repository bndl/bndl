import copy

from bndl.compute import driver
from bndl.util.objects import LazyObject
from bndl.util.supervisor import split_args


__all__ = ['ctx']


def create_ctx():
    argparser = copy.copy(driver.argparser)
    argparser.prog = 'bndl.compute.script'
    argparser.epilog = 'Use -- before bndl arguments to separate the from' \
                       'arguments to the main program.'
    args = argparser.parse_args(split_args())
    return driver.main(args, daemon=True)


ctx = LazyObject(create_ctx)
