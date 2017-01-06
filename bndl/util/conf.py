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

from collections import Iterable
from configparser import ConfigParser
from functools import lru_cache
import importlib
import os
import shlex
import sys


BNDL_ENV_KEY = 'BNDL_CONF'
_NOT_SET = object()


_SETTINGS_CACHE = {}


class Config(object):
    def __init__(self, values=None, use_environment=True, **kwargs):
        if use_environment:
            self.values = {}

            # read from .bndl.ini files
            config = ConfigParser()
            config.read([
                '~/.bndl.ini',
                '~/bndl.ini',
                './.bndl.ini',
                './bndl.ini',
            ])
            for section in config.sections():
                for key, value in config[section].items():
                    self.values['%s.%s' % (section, key)] = value

            # read from BNDL_CONF environment variable
            env_config = os.environ.get(BNDL_ENV_KEY, '')
            for option in shlex.split(env_config):
                option = option.split('=')
                if len(option) != 2:
                    raise RuntimeError('%s not in key=value format in BNDL_CONFIG environment variable' % option)
                self[option[0]] = option[1]

            if values:
                # override with config provided through the constructor
                self.values.update(values)
        else:
            self.values = values or {}

        if kwargs:
            self.values.update(kwargs)


    @lru_cache(1024)
    def _get_setting(self, key):
        pkg, *attr = key.rsplit('.', 1)
        if attr:
            attr = attr[0]
            mod = sys.modules.get(pkg)
            if not mod:
                try:
                    mod = importlib.import_module(pkg)
                except ImportError:
                    ...
            if mod:
                return getattr(mod, attr)

    def get(self, key, fmt=None, default=_NOT_SET):
        setting = self._get_setting(key)
        value = self.values.get(key, _NOT_SET)
        if value is _NOT_SET:
            if setting and default is _NOT_SET:
                default = setting.default
            if default is not _NOT_SET:
                return default
            else:
                return None
        else:
            if fmt is None and setting:
                fmt = setting.fmt
            return fmt(value) if fmt else value

    def get_int(self, *args, **kwargs):
        return self.get(*args, fmt=int, **kwargs)

    def get_float(self, *args, **kwargs):
        return self.get(*args, fmt=float, **kwargs)

    def get_bool(self, *args, **kwargs):
        return self.get(*args, fmt=bool, **kwargs)

    def get_str(self, *args, **kwargs):
        return self.get(*args, fmt=str, **kwargs)

    def get_attr(self, *args, obj, **kwargs):
        attr = self.get(*args, fmt=str, **kwargs)
        return getattr(obj, attr)

    def set(self, key, value):
        self.values[key] = value
        return self

    def update(self, *args, **kwargs):
        for arg in args:
            if isinstance(arg, tuple):
                assert len(arg) == 2
                self.set(*arg)
            elif isinstance(arg, str):
                try:
                    key, value = arg.split('=', 1)
                except ValueError:
                    raise ValueError('%r is not formatted as "key=value"' % arg)
                self.set(key.strip(), value.strip())
            else:
                assert False
        self.values.update(kwargs)

    __getitem__ = get
    __setitem__ = set

    def __repr__(self):
        return '<Conf %r>' % self.values

    def __reduce__(self):
        return Config, (self.values, False)


class Setting(object):
    default = None
    fmt = None
    desc = None

    def __init__(self, default=_NOT_SET, fmt=None, desc=None):
        self.default = default
        if default != _NOT_SET and desc:
            desc += ' Defaults to %r.' % default
        self.desc = desc
        self.__doc__ = desc
        if fmt is not None:
            assert callable(fmt)
            self.fmt = fmt


class String(Setting):
    pass


class Bool(Setting):
    def fmt(self, v):
        if type(v) is bool:
            return v
        else:
            return str(v).lower() in ('1', 'true', 'yes')


class Int(Setting):
    fmt = int


class Float(Setting):
    fmt = float


class CSV(String):
    def fmt(self, v):
        if isinstance(v, Iterable) and not isinstance(v, str):
            return v
        return list(e.strip() for e in v.split(','))


class Attr(Setting):
    def __init__(self, *args, obj=None, **kwargs):
        assert obj is not None
        super().__init__(*args, **kwargs)
        self.obj = obj

    def fmt(self, v):
        return getattr(self.obj, str(v), self.default)


class Enum(Setting):
    def __init__(self, *args, choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.choices = choices

    def fmt(self, v):
        if v in self.choices:
            return v
        else:
            raise ValueError('Unsupported value %s (must be one of %r)' % (v, self.choices))
