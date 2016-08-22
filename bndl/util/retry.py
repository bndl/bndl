import time


def retry_delay(timeout_backoff, retry_round):
    assert timeout_backoff > 1
    assert retry_round >= 1
    return timeout_backoff ** retry_round - 1


def do_with_retry(action, limit=1, backoff=None, transients=(Exception,)):
    assert not backoff or backoff > 1
    assert not limit or limit >= 0

    fails = 0

    while True:
        try:
            return action()
        except transients:
            fails += 1
            if not limit or fails > limit:
                raise
            elif backoff:
                sleep = retry_delay(backoff, fails)
                time.sleep(sleep)
