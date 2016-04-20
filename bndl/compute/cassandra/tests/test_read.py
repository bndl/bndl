from bndl.compute.cassandra.tests import CassandraTest


class ReadTest(CassandraTest):
    rows = [dict(key=str(i), cluster=str(i), varint_val=i) for i in range(100)]

    def setUp(self):
        super().setUp()
        self.ctx.collection(self.rows).cassandra_save(self.keyspace, self.table).execute()

    def test_count(self):
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(), len(self.rows))
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=False), len(self.rows))
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), len(self.rows))
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).cache().count(), len(self.rows))

    def test_collect_dicts(self):
        self.ctx.cassandra_table(self.keyspace, self.table).as_dicts().collect()

    def test_collect_tuples(self):
        self.ctx.cassandra_table(self.keyspace, self.table).as_tuples().collect()

    # TODO def test_select(self):
    # TODO def test_where(self):

    def test_slicing(self):
        self.ctx.cassandra_table(self.keyspace, self.table).first()
        self.ctx.cassandra_table(self.keyspace, self.table).take(3)
