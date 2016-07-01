from bndl.compute.cassandra.session import _prepare
from bndl.compute.cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        hits_count_start = self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum()

        # trigger query preparation
        dset = self.ctx.cassandra_table(self.keyspace, self.table)
        for i in range(self.ctx.worker_count):
            targeted = dset.allow_workers(lambda workers: [workers[i]])
            self.assertEqual(targeted.collect(), [])

        # check that prepared query is cached
        hits_count = self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum()
        self.assertEqual(hits_count - hits_count_start , 16)  # @UndefinedVariable
