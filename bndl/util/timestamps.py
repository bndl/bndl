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

# from https://docs.python.org/3/library/datetime.html
from datetime import timedelta, tzinfo, date, datetime


ZERO_TIME = timedelta(0)


class UTC(tzinfo):
    def utcoffset(self, dt):
        return ZERO_TIME

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO_TIME

    def __reduce__(self, *args, **kwargs):
        return UTC, ()

    def __repr__(self):
        return self.__class__.__name__


def timestamp_micros(value):
    if isinstance(value, datetime):
        return int((value - datetime(1970, 1, 1)).total_seconds() * 1000000)
    elif isinstance(value, date):
        return int((value - date(1970, 1, 1)).total_seconds() * 1000000)
    else:
        raise ValueError(type(value) + ' unsupported')

def timestamp_millis(value):
    return timestamp_micros(value) // 1000

ms_timestamp = timestamp_millis
