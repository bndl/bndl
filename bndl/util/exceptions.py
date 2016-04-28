from contextlib import contextmanager

@contextmanager
def catch(*exc):
    try:
        yield
    except Exception as e:
        if exc and e not in exc:
            raise e
