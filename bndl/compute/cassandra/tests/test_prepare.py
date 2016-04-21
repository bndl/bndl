from bndl.compute.cassandra.session import _prepare
from bndl.compute.cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).collect(), [])
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).collect(), [])
        self.assertEqual(self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum(), 16)  # @UndefinedVariable
