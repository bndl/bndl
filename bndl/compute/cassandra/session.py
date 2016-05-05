from collections import Sequence
import contextlib
from functools import lru_cache, partial
from threading import Lock

from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from bndl.util.pool import ObjectPool
from cassandra.cluster import Cluster, Session
from cassandra.policies import TokenAwarePolicy
import queue


_prepare_lock = Lock()

@lru_cache()
def _prepare(self, query, custom_payload=None):
    return Session.prepare(self, query, custom_payload)

def prepare(self, query, custom_payload=None):
    with _prepare_lock:
        return _prepare(self, query, custom_payload)


def get_contact_points(ctx, contact_points):
    if isinstance(contact_points, str):
        contact_points = (contact_points,)
    return _get_contact_points(ctx, *(contact_points or ()))

@lru_cache()
def _get_contact_points(ctx, *contact_points):
    if not contact_points:
        contact_points = ctx.conf.get('cassandra.contact_points')
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
        def create():
            '''create a new session'''
            cluster = Cluster(contact_points)
            cluster.load_balancing_policy = TokenAwarePolicy(LocalNodeFirstPolicy(ctx.node.ip_addresses))
            session = cluster.connect(keyspace)
            session.prepare = partial(prepare, session)
            return session
        def check(session):
            '''check if the session is not closed'''
            return not session.is_shutdown
        pools[contact_points] = pool = ObjectPool(create, check, max_size=4)
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
