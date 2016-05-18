from collections import Sequence
import contextlib
from functools import lru_cache, partial
import queue
from threading import Lock

from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from bndl.util.pool import ObjectPool
from cassandra.cluster import Cluster, Session
from cassandra.policies import TokenAwarePolicy, RetryPolicy, WriteType
from bndl.compute.cassandra import conf


class MultipleRetryPolicy(RetryPolicy):
    def __init__(self, read_retries=0, write_retries=0):
        self.read_retries = read_retries
        self.write_retries = write_retries

    def on_read_timeout(self, query, consistency, required_responses,
        received_responses, data_retrieved, retry_num):
        if retry_num < self.read_retries or received_responses >= required_responses and not data_retrieved:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, query, consistency, write_type,
        required_responses, received_responses, retry_num):
        if retry_num < self.write_retries or write_type in (WriteType.SIMPLE, WriteType.BATCH_LOG):
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)


_PREPARE_LOCK = Lock()


@lru_cache()
def _prepare(self, query, custom_payload=None):
    return Session.prepare(self, query, custom_payload)


def prepare(self, query, custom_payload=None):
    with _PREPARE_LOCK:
        return _prepare(self, query, custom_payload)


def get_contact_points(ctx, contact_points):
    if isinstance(contact_points, str):
        contact_points = (contact_points,)
    return _get_contact_points(ctx, *(contact_points or ()))


@lru_cache()
def _get_contact_points(ctx, *contact_points):
    if not contact_points:
        contact_points = ctx.conf.get(conf.CONTACT_POINTS, defaults=conf.DEFAULTS)
    if not contact_points:
        contact_points = set()
        for worker in ctx.workers:
            contact_points |= worker.ip_addresses
    if not contact_points:
        contact_points = ctx.node.ip_addresses
    if isinstance(contact_points, str):
        contact_points = [contact_points]
    if isinstance(contact_points, Sequence) and len(contact_points) == 1 and isinstance(contact_points[0], str):
        contact_points = contact_points[0].split(',')
    return tuple(sorted(contact_points))


@contextlib.contextmanager
def cassandra_session(ctx, keyspace=None, contact_points=None):
    # get hold of the dict of pools (keyed by contact_points)
    pools = getattr(cassandra_session, 'pools', None)
    if not pools:
        cassandra_session.pools = pools = {}
    # determine contact points, either given or ip addresses of the workers
    contact_points = get_contact_points(ctx, contact_points)
    # check if there is a cached session object
    pool = pools.get(contact_points)
    # or create one if not or that session is shutdown
    if not pool:
        retry_policy = MultipleRetryPolicy(ctx.conf.get_int(conf.READ_RETRY_COUNT, defaults=conf.DEFAULTS),
                                           ctx.conf.get_int(conf.WRITE_RETRY_COUNT, defaults=conf.DEFAULTS))

        cluster = Cluster(
            contact_points,
            port=ctx.conf.get_int(conf.PORT, defaults=conf.DEFAULTS),
            compression=ctx.conf.get_bool(conf.COMPRESSION, defaults=conf.DEFAULTS),
            load_balancing_policy=TokenAwarePolicy(LocalNodeFirstPolicy(ctx.node.ip_addresses)),
            default_retry_policy=retry_policy,
            metrics_enabled=ctx.conf.get_bool(conf.METRICS_ENABLED, defaults=conf.DEFAULTS),
        )

        def create(cluster=cluster):
            '''create a new session'''
            session = cluster.connect(keyspace)
            session.prepare = partial(prepare, session)
            session.default_fetch_size = ctx.conf.get_int(conf.FETCH_SIZE_ROWS, defaults=conf.DEFAULTS)

            return session

        def check(session):
            '''check if the session is not closed'''
            return not session.is_shutdown

        pools[contact_points] = pool = ObjectPool(create, check, max_size=4)
        pool.cluster = cluster

    # take a session from the pool, yield it to the caller
    # and put the session back in the pool
    session = pool.get()
    try:
        yield session
    finally:
        try:
            pool.put(session)
        except queue.Full:
            session.shutdown()
