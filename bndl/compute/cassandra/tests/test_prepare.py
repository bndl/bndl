from bndl.compute.cassandra.session import _prepare
from bndl.compute.cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        with self.assertRaises(StopIteration):
            self.ctx.cassandra_table(self.keyspace, self.table).first()
        with self.assertRaises(StopIteration):
            self.ctx.cassandra_table(self.keyspace, self.table).first()
        self.assertEqual(self.ctx.range(4, pcount=4).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum(), 1)  # @UndefinedVariable
