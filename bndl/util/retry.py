import time


def retry_delay(timeout_backoff, retry_round):
    assert timeout_backoff > 1
    assert retry_round >= 1
    return timeout_backoff ** retry_round - 1


def do_with_retry(action, retry_limit=1, retry_backoff=None, transients=(Exception,)):
    fails = 0

    while True:
        try:
            return action()
        except transients:
            fails += 1
            if not retry_limit or fails > retry_limit:
                raise
            elif retry_backoff:
                sleep = retry_delay(retry_backoff, fails)
                time.sleep(sleep)
