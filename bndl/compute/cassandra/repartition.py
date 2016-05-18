# check support first with cluster.metadata.can_support_partitioner()
# prep a query once: select = session.prepare('select * from adg.authorship where doc_id=?')
# bind a row bound_select = select.bind(dict(doc_id=1, ...))
# get from bound query key = bound_select.routing_key
# get replicaset through cluster.metadata.get_replicas(keyspace, key)
