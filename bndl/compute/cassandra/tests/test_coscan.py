from bndl.compute.cassandra.tests import CassandraTest


class PrimaryKeyCoScanTest(CassandraTest):
    postfixes = ('a', 'b', 'c')
    tables = ['test_coscan_%s' % t for t in postfixes]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with cls.ctx.cassandra_session() as session:
            for table in cls.tables:
                session.execute('''
                    create table if not exists {keyspace}.{table} (
                        key text,
                        cluster text,
                        val text,
                        primary key (key, cluster)
                    );
                '''.format(keyspace=cls.keyspace, table=table))


    def setUp(self):
        super().setUp()
        with self.ctx.cassandra_session() as session:
            for table in self.tables:
                session.execute('truncate {keyspace}.{table};'.format(keyspace=self.keyspace, table=table))

        for postfix, table in zip(self.postfixes, self.tables):
            self.ctx.range(20).map(lambda i: dict(key=str(i % 10), cluster=str(i), val='%s-%s' % (postfix, i))).cassandra_save(self.keyspace, table).execute()


    def test_read(self):
        dset = self.ctx.cassandra_table(self.keyspace, self.tables[0])
        for table in self.tables[1:]:
            dset = dset.coscan(self.ctx.cassandra_table(self.keyspace, table))

        for key, group in dset.collect():
            print(key, group)


#     def test_count(self):
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=False), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).cache().count(), len(self.rows))

#     def test_collect_dicts(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).as_dicts().collect()
#
#     def test_collect_tuples(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).as_tuples().collect()

    # TODO def test_select(self):
    # TODO def test_where(self):

#     def test_slicing(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).first()
#         self.ctx.cassandra_table(self.keyspace, self.table).take(3)





class PartitionKeyCoScanTest(CassandraTest):
    tables = ['test_coscan_partitionkey_%s' % t for t in ('a', 'b')]
    table_defs = [
        '''
          create table if not exists {{keyspace}}.{table} (
              pk text primary key,
              val text
          );
        '''.format(table=tables[0]),
        '''
          create table if not exists {{keyspace}}.{table} (
              key text,
              cluster text,
              val text,
              primary key (key, cluster)
          );
        '''.format(table=tables[1]),
    ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with cls.ctx.cassandra_session() as session:
            for table_def in cls.table_defs:
                session.execute(table_def.format(keyspace=cls.keyspace))


    def setUp(self):
        super().setUp()
        with self.ctx.cassandra_session() as session:
            for table in self.tables:
                session.execute('truncate {keyspace}.{table};'.format(keyspace=self.keyspace, table=table))

        self.ctx.range(10).map(lambda i: dict(pk=str(i), val='%s-%s' % ('a', i))).cassandra_save(self.keyspace, self.tables[0]).execute()
        self.ctx.range(20).map(lambda i: dict(key=str(i % 10), cluster=str(i), val='%s-%s' % ('b', i))).cassandra_save(self.keyspace, self.tables[1]).execute()


    def test_read(self):
        a = self.ctx.cassandra_table(self.keyspace, self.tables[0])

        print(a.collect())

        b = self.ctx.cassandra_table(self.keyspace, self.tables[1])
        dset = a.coscan(b, keys=('pk', 'key'))

        for key, group in dset.collect():
            print(key, group)


#     def test_count(self):
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=False), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), len(self.rows))
#         self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).cache().count(), len(self.rows))

#     def test_collect_dicts(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).as_dicts().collect()
#
#     def test_collect_tuples(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).as_tuples().collect()

    # TODO def test_select(self):
    # TODO def test_where(self):

#     def test_slicing(self):
#         self.ctx.cassandra_table(self.keyspace, self.table).first()
#         self.ctx.cassandra_table(self.keyspace, self.table).take(3)
