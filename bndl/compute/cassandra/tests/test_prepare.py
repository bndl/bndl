from bndl.compute.cassandra.session import _prepare
from bndl.compute.cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        # trigger query preparation
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).collect(), [])
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).collect(), [])
        # check that prepared query is cached
        hits_count = self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum()
        self.assertEqual(hits_count, 16)  # @UndefinedVariable
