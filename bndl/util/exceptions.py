from contextlib import contextmanager


@contextmanager
def catch(*ignore):
    try:
        yield
    except Exception as exc:
        if ignore and exc not in ignore:
            raise exc
