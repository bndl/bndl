from types import MethodType


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



class Property(object):
    def __init__(self, p):
        self.p = p
        self.__doc__ = p.__doc__

    def __get__(self, obj, objtype=None):
        return self.p.fget(obj._extended)

    def __set__(self, obj, value):
        return self.p.fset(obj._extended, value)

    def __delete__(self, obj):
        return self.p.fdel(obj._extended)


class Function(object):
    def __init__(self, f):
        self.f = f
        self.__doc__ = f.__doc__

    def __get__(self, obj, objtype=None):
        return MethodType(self.f, obj._extended)


class ExtensionGroupMeta(type):
    def __new__(cls, name, parents, dct):
        for key, value in dct.items():
            if key[0] != '_':
                if callable(value):
                    dct[key] = Function(value)
                elif isinstance(value, property):
                    dct[key] = Property(value)
                else:
                    raise NotImplementedError('Unable to proxy %s of type %s' %
                                         (key, value))
        return super().__new__(cls, name, parents, dct)


class ExtensionGroup(metaclass=ExtensionGroupMeta):
    def __init__(self, extended):
        self._extended = extended
