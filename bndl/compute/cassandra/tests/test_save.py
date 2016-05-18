from bndl.compute.cassandra.tests import CassandraTest


class SaveTest(CassandraTest):
    num_rows = 10000

    def setUp(self):
        super().setUp()
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), 0)


    def test_save_dicts(self):
        dset = self.ctx.range(self.num_rows).map(lambda i: dict(key=str(i), cluster=str(i), varint_val=i))
        saved = (dset.cassandra_save(self.keyspace, self.table).sum())

        self.assertEqual(saved, self.num_rows)
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), self.num_rows)

        rows = self.ctx.cassandra_table(self.keyspace, self.table) \
                   .select('key', 'cluster', 'varint_val').as_dicts().collect()
        self.assertEqual(len(rows), len(dset.collect()))
        self.assertEqual(sorted(rows, key=lambda row: int(row['key'])), dset.collect())


    def test_save_tuples(self):
        dset = self.ctx.range(self.num_rows).map(lambda i: (str(i), str(i), i))
        saved = (dset.cassandra_save(
            self.keyspace, self.table,
            columns=('key', 'cluster', 'varint_val'),
            keyed_rows=False
        ).sum())

        self.assertEqual(saved, self.num_rows)
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), self.num_rows)

        rows = self.ctx.cassandra_table(self.keyspace, self.table) \
                       .select('key', 'cluster', 'varint_val').as_tuples().collect()
        self.assertEqual(len(rows), len(dset.collect()))
        self.assertEqual(sorted(rows, key=lambda row: int(row[0])), dset.collect())
