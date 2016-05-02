import contextlib
from functools import lru_cache, partial

from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from cassandra.cluster import Cluster, Session
from cassandra.policies import TokenAwarePolicy
from threading import Lock
from collections import Sequence
from weakref import WeakValueDictionary


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
    if isinstance(contact_points, Sequence) and len(contact_points) == 1 and isinstance(contact_points[0], str):
        contact_points = contact_points[0].split(',')
    return tuple(sorted(contact_points))


@contextlib.contextmanager
def cassandra_session(ctx, keyspace=None, contact_points=None):
    # keep a weak reference to existing cluster objects
    clusters = getattr(cassandra_session, 'clusters', None)
    if clusters is None:
        cassandra_session.clusters = clusters = WeakValueDictionary()
    # determine contact points, either given or ip addresses of the workers
    contact_points = get_contact_points(ctx, contact_points)
    # check if there is a cached cluster object
    cluster = clusters.get(contact_points)
    # or create one if not or that session is shutdown
    if not cluster or cluster.is_shutdown:
        clusters[contact_points] = cluster = Cluster(contact_points)
        cluster.load_balancing_policy = TokenAwarePolicy(LocalNodeFirstPolicy(ctx.node.ip_addresses))
    # created a session
    session = cluster.connect(keyspace)
    session.prepare = partial(prepare, session)
    # provided it to users
    yield session
    # and close the session
    session.shutdown()
