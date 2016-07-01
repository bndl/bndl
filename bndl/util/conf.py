from configparser import ConfigParser
import os
import shlex


BNDL_ENV_KEY = 'BNDL_CONF'
_MISSING = object()


class Config(object):
    def __init__(self, values={}):
        self.values = {}

        # read from .bndl.ini files
        config = ConfigParser()
        config.read(['~/.bndl.ini',
                     './.bndl.ini', ])
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

        # override with config provided through the constructor
        self.values.update(values)


    def get(self, key, fmt=None, defaults=None):
        value = self.values.get(key, _MISSING)
        if value == _MISSING:
            value = defaults.get(key, _MISSING)
        if value == _MISSING:
            raise KeyError(key)
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

    def __setitem__(self, key, value):
        self.values[key] = value
        return self

    def __getitem__(self, key):
        return self.values[key]

    def __repr__(self):
        return '<Conf %r>' % self.values
