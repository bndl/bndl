from contextlib import contextmanager


@contextmanager
def catch(*ignore):
    try:
        yield
    except Exception as exc:
        if ignore and not any(isinstance(exc, i) for i in ignore):
            raise exc
