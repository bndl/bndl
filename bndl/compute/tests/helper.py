from bndl import conf
from bndl.compute.context import ComputeContext
from bndl.compute.worker import start_worker


def start_test_ctx(executor_count):
    conf['bndl.net.aio.uvloop'] = False
    conf['bndl.compute.executor_count'] = 0
    conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.1:0'

    ctx = ComputeContext.create()
    workers = []

    if executor_count > 0:
        conf['bndl.net.seeds'] = ctx.node.addresses
        n_workers = executor_count // 2 + 1

        for i in range(n_workers):
            conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.%s:0' % (i + 2)
            worker = start_worker()
            workers.append(worker)

        for i in range(executor_count):
            worker = workers[i % n_workers]
            worker.start_executors(1)

    for _ in range(2):
        ctx.await_executors(executor_count, 20, 120)

    assert ctx.executor_count == executor_count, '%s != %s' % (ctx.executor_count, executor_count)
    for ex in ctx.executors:
        assert not ex.ip_addresses() & ctx.node.ip_addresses()

    return ctx, workers


def stop_test_ctx(ctx, workers):
    ctx.stop()
    try:
        for w in workers:
            w.stop_async().result(60)
    except TimeoutError:
        pass


test_ctx = None
test_workers = None

def global_test_ctx(executor_count):
    global test_ctx, test_workers
    if test_ctx is None:
        assert not test_workers
        test_ctx, test_workers = start_test_ctx(executor_count)
    return test_ctx, test_workers


def get_global_test_ctx():
    return test_ctx, test_workers
    

def stop_global_test_ctx(executor_count):
    global test_ctx, test_workers
    stop_test_ctx(test_ctx, test_workers)
