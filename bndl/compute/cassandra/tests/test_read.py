from bndl.compute.cassandra.tests import CassandraTest


class ReadTest(CassandraTest):
    rows = [dict(key=str(i), cluster=str(i), varint_val=i) for i in range(100)]

    def setUp(self):
        super().setUp()
        self.ctx.collection(self.rows).cassandra_save(self.keyspace, self.table).execute()

    def test_count(self):
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=False), len(self.rows))
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), len(self.rows))

    def test_cache(self):
        dset = self.ctx.cassandra_table(self.keyspace, self.table)
        self.assertEqual(dset.count(), len(self.rows))  # count before
        self.assertEqual(dset.cache().count(), len(self.rows))  # count while caching
        self.assertEqual(dset.count(), len(self.rows))  # count after

    def test_collect_dicts(self):
        dicts = self.ctx.cassandra_table(self.keyspace, self.table).as_dicts()
        self.assertEqual(len(dicts.collect()), len(self.rows))
        self.assertEqual(type(dicts.first()), dict)

    def test_collect_tuples(self):
        tuples = self.ctx.cassandra_table(self.keyspace, self.table).as_tuples()
        self.assertEqual(len(tuples.collect()), len(self.rows))
        self.assertEqual(type(tuples.first()), tuple)

    # TODO def test_select(self):
    # TODO def test_where(self):

    def test_slicing(self):
        first = self.ctx.cassandra_table(self.keyspace, self.table).first()
        self.assertIn({k: v for k, v in first._asdict().items() if k in ('key', 'cluster', 'varint_val')}, self.rows)
        self.assertEqual(len(self.ctx.cassandra_table(self.keyspace, self.table).take(3)), 3)
