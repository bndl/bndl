# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    def __init__(self, factory, destructor=None, attrs=None):
        self._factory = factory
        self._destructor_key = destructor
        self._attrs = attrs or {}


    def _materialize(self):
        factory = object.__getattribute__(self, '_factory')
        destructor_key = object.__getattribute__(self, '_destructor_key')

        obj = factory()
        if not obj:
            raise RuntimeError('Unable to create lazy object from %s' % factory)

        if destructor_key:
            destructor = getattr(obj, destructor_key)

        self.__class__ = obj.__class__
        for k, v in obj.__dict__.items():
            self.__dict__[k] = v

        if destructor_key:
            setattr(self, destructor_key, _Destructor(self, factory, destructor, destructor_key))


    def __getitem__(self, name):
        object.__getattribute__(self, '_materialize')()
        return self[name]


    def __setitem__(self, name, value):
        object.__getattribute__(self, '_materialize')()
        self[name] = value


    def __getattribute__(self, name):
        attrs = object.__getattribute__(self, '_attrs')
        if name in attrs:
            return attrs[name]
        object.__getattribute__(self, '_materialize')()
        return object.__getattribute__(self, name)



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
