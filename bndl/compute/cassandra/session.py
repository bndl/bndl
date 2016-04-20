import contextlib
from functools import lru_cache, partial

from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from cassandra.cluster import Cluster, Session
from cassandra.policies import TokenAwarePolicy


@lru_cache()
def prepare(self, query, custom_payload=None):
    return Session.prepare(self, query, custom_payload)


@contextlib.contextmanager
def cassandra_session(ctx, keyspace=None):
    session = cassandra_session.__dict__.get('session')
    if not session or session.is_shutdown:
        contact_points = ctx.conf.get(
            'cassandra.contact_points',
            ctx.node.ip_addresses
        )
        if isinstance(contact_points, str):
            contact_points = contact_points.split(',')
        cluster = Cluster(contact_points)
        cluster.load_balancing_policy = TokenAwarePolicy(LocalNodeFirstPolicy(ctx.node.ip_addresses))
        session = cluster.connect(keyspace)
        session.__dict__['prepare'] = partial(prepare, session)
        cassandra_session.__dict__['session'] = session

    yield session
