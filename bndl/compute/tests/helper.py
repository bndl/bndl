from bndl import conf
from bndl.compute.context import ComputeContext
from bndl.compute.worker import start_worker
from bndl.util.strings import random_id


def start_test_ctx(executor_count):
    conf['bndl.net.aio.uvloop'] = False
    conf['bndl.compute.executor_count'] = 0
    conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.1:0'
    conf['bndl.net.cluster'] = random_id()

    ctx = ComputeContext.create()
    ComputeContext.instances.remove(ctx)

    workers = [ctx.node]

    conf['bndl.net.seeds'] = ctx.node.addresses

    n_workers = min(max(2, executor_count // 4 - 1), executor_count)
    for i in range(n_workers):
        conf['bndl.net.listen_addresses'] = 'tcp://127.0.0.%s:0' % (i + 2)
        conf['bndl.net.machine'] = i + 1
        worker = start_worker()
        workers.append(worker)

    n_workers = len(workers)

    futs = []
    for w in workers:
        futs.append(w.start_executors(executor_count // n_workers))
    for i in range(executor_count % n_workers):
        futs.append(workers[i].start_executors(1))
    for fut in futs:
        fut.result()

    workers.remove(ctx.node)

    for _ in range(2):
        ctx.await_executors(executor_count, 20, 120)

    assert ctx.executor_count == executor_count, '%s != %s' % (ctx.executor_count, executor_count)

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


def stop_global_test_ctx():
    global test_ctx, test_workers
    stop_test_ctx(test_ctx, test_workers)
