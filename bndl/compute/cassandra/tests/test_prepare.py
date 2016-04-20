from bndl.compute.cassandra.session import prepare
from bndl.compute.cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        with self.assertRaises(StopIteration):
            self.ctx.cassandra_table(self.keyspace, self.table).first()
        with self.assertRaises(StopIteration):
            self.ctx.cassandra_table(self.keyspace, self.table).first()
        self.assertEqual(self.ctx.range(4).map_partitions(lambda p: [prepare.cache_info()[3]]).sum(), 1)  # @UndefinedVariable
