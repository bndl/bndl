import contextlib
from functools import lru_cache, partial

from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from cassandra.cluster import Cluster, Session
from cassandra.policies import TokenAwarePolicy
from threading import Lock
from itertools import chain


_prepare_lock = Lock()

@lru_cache()
def _prepare(self, query, custom_payload=None):
    return Session.prepare(self, query, custom_payload)

def prepare(self, query, custom_payload=None):
    with _prepare_lock:
        return _prepare(self, query, custom_payload)


@lru_cache()
def get_contact_points(ctx, *contact_points):
    if not contact_points:
        contact_points = ctx.conf.get('cassandra.contact_points')
    if not contact_points:
        contact_points = set()
        for worker in ctx.workers:
            contact_points |= worker.ip_addresses
    if not contact_points:
        contact_points = ctx.node.ip_addresses
    if isinstance(contact_points, str):
        contact_points = contact_points.split(',')
    return tuple(sorted(contact_points))


@contextlib.contextmanager
def cassandra_session(ctx, keyspace=None, contact_points=None):
    sessions = getattr(cassandra_session, 'sessions', None)
    if sessions is None:
        cassandra_session.sessions = sessions = {}
    # determine contact points, either given or ip addresses of the workers
    contact_points = get_contact_points(ctx, *(contact_points or ()))
    # check if there is a cached session
    session = sessions.get(contact_points)
    # or create one if not or that session is shutdown
    if not session or session.is_shutdown:
        cluster = Cluster(contact_points)
        cluster.load_balancing_policy = TokenAwarePolicy(LocalNodeFirstPolicy(ctx.node.ip_addresses))
        session = cluster.connect(keyspace)
        session.prepare = partial(prepare, session)
        sessions[contact_points] = session

    yield session
