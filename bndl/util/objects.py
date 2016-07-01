class LazyObject(object):
    def __init__(self, factory):
        self._factory = factory

    def __getattribute__(self, name):
        factory = object.__getattribute__(self, '_factory')

        obj = factory()
        if not obj:
            raise RuntimeError('Unable to create lazy object from %s' % factory)
        self.__class__ = obj.__class__
        self.__dict__ = obj.__dict__

        return object.__getattribute__(obj, name)
