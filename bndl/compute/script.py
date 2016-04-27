from bndl.compute import driver
from bndl.util.supervisor import  split_args
import copy


__all__ = ['ctx']


class LazyObject(object):
    def __init__(self, factory):
        self._factory = factory

    def __getattribute__(self, name, *args, **kwargs):
        try:
            w = object.__getattribute__(self, '_wrapped')
        except AttributeError:
            f = object.__getattribute__(self, '_factory')
            w = f()
            object.__setattr__(self, '_wrapped', w)
        return object.__getattribute__(w, name, *args, **kwargs)

    def __setattribute__(self, name, *args, **kwargs):
        return self.wrapped.__setattr__(name, *args, **kwargs)


def create_ctx():
    argparser = copy.copy(driver.argparser)
    argparser.prog = 'bndl.compute.script'
    args = argparser.parse_args(split_args())
    return driver.main(args, daemon=True)


ctx = LazyObject(create_ctx)
