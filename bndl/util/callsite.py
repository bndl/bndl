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

from functools import wraps
import contextlib
import inspect
import linecache
import os.path
import sys
import threading


_callsite = threading.local()

def _get_callsite(*internal, name=None):
    internal = [inspect.getfile(i) if not isinstance(i, str) else i for i in internal] + [__file__]
    internal = [os.path.dirname(fname) for fname in internal]

    frame = sys._getframe().f_back.f_back
    name_override = name
    name = None
    desc = None

    while frame:
        co = frame.f_code
        file = co.co_filename
        func = co.co_name
        internals = any(map(file.startswith, internal))
        if internals and func[0] != '_':
            name = func
        if not internals:
            line = frame.f_lineno
            text = linecache.getline(file, line).strip()
            desc = file, line, func, text
            break
        frame = frame.f_back

    return name_override or name, desc

def get_callsite(*internal, name=None):
    if hasattr(_callsite, 'current'):
        return _callsite.current
    else:
        return _get_callsite(*internal, name=name)


@contextlib.contextmanager
def set_callsite(*internal, name=None):
    internal += (contextlib.contextmanager,)
    if not hasattr(_callsite, 'current'):
        _callsite.current = get_callsite(*internal, name=name)
        try:
            yield
        finally:
            del _callsite.current
    else:
        yield


def callsite(*internal, name=None):
    def decorator(func):
        nonlocal internal
        internal += (func,)
        @wraps(func)
        def wrapper(*args, **kwargs):
            with set_callsite(*internal, name=name or func.__name__):
                return func(*args, **kwargs)
        return wrapper
    return decorator
