class _Destructor(object):
    def __init__(self, obj, factory, destructor, destructor_key):
        self.obj = obj
        self.factory = factory
        self.destructor = destructor
        self.destructor_key = destructor_key

    def __call__(self, *args, **kwargs):
        self.destructor(*args, **kwargs)
        self.obj.__class__ = LazyObject



class LazyObject(object):
    def __init__(self, factory, destructor=None):
        self._factory = factory
        self._destructor_key = destructor

    def __getattribute__(self, name):
        factory = object.__getattribute__(self, '_factory')
        destructor_key = object.__getattribute__(self, '_destructor_key')

        obj = factory()
        if not obj:
            raise RuntimeError('Unable to create lazy object from %s' % factory)

        if destructor_key:
            destructor = getattr(obj, destructor_key)

        self_orig = self
        self.__class__ = obj.__class__
        for k, v in obj.__dict__.items():
            self.__dict__[k] = v

        if destructor_key:
            setattr(self, destructor_key, _Destructor(self, factory, destructor, destructor_key))

        return object.__getattribute__(self_orig, name)
