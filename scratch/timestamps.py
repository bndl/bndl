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


def ms_timestamp(value):
    if isinstance(value, datetime):
        return int((value - datetime(1970, 1, 1)).total_seconds() * 1000)
    elif isinstance(value, date):
        return int((value - date(1970, 1, 1)).total_seconds() * 1000)
    else:
        raise ValueError(type(value) + ' unsupported')
