class LazyObject(object):
    def __init__(self, factory):
        self._factory = factory

    def __getattribute__(self, name, *args, **kwargs):
        try:
            wrapped = object.__getattribute__(self, '_wrapped')
        except AttributeError:
            factory = object.__getattribute__(self, '_factory')
            wrapped = factory()
            object.__setattr__(self, '_wrapped', wrapped)
        return object.__getattribute__(wrapped, name, *args, **kwargs)

    def __setattribute__(self, name, *args, **kwargs):
        return self.wrapped.__setattr__(name, *args, **kwargs)
