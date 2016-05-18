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
            def create_row(i, postfix=postfix):
                return dict(key=str(i % 10), cluster=str(i), val='%s-%s' % (postfix, i))
            self.ctx.range(20).map(create_row).cassandra_save(self.keyspace, table).execute()


    def test_read(self):
        dset = self.ctx.cassandra_table(self.keyspace, self.tables[0])
        for table in self.tables[1:]:
            dset = dset.coscan(self.ctx.cassandra_table(self.keyspace, table))

        grouped = dset.collect()
        self.assertEqual(len(grouped), 20)

        keys = []
        for key, group in grouped:
            keys.append(key)
            self.assertEqual(len(group), 3)
            for postfix, column in zip(self.postfixes, group):
                self.assertEqual(column.key, key[0])
                self.assertEqual(column.cluster, key[1])
                self.assertEqual(column.val, postfix + '-' + key[1])

        self.assertEqual(sorted(keys), sorted(set(keys)))
        self.assertEqual(sorted(keys), sorted((str(i % 10), str(i)) for i in range(20)))


class PartitionKeyCoScanTest(CassandraTest):
    postfixes = ('a', 'b')
    tables = ['test_coscan_partitionkey_%s' % t for t in postfixes]
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

        self.ctx.range(10).map(lambda i: dict(pk=str(i), val='%s-%s' % ('a', i))) \
            .cassandra_save(self.keyspace, self.tables[0]).execute()

        self.ctx.range(20).map(lambda i: dict(key=str(i % 10), cluster=str(i), val='%s-%s' % ('b', i))) \
            .cassandra_save(self.keyspace, self.tables[1]).execute()


    def test_read(self):
        left = self.ctx.cassandra_table(self.keyspace, self.tables[0])
        right = self.ctx.cassandra_table(self.keyspace, self.tables[1])
        dset = left.coscan(right, keys=('pk', 'key'))

        grouped = dset.collect()
        self.assertEqual(len(grouped), 10)

        keys = []
        for key, group in grouped:
            keys.append(key)
            self.assertEqual(len(group), 2)
            self.assertEqual((group[0].pk,), key)
            self.assertEqual(group[0].val.split('-'), [self.postfixes[0], key[0]])
            for column in group[1]:
                self.assertEqual((column.key,), key)
                self.assertEqual(column.val, self.postfixes[1] + '-' + column.cluster)
            clusters = sorted(int(column.cluster) for column in group[1])
            self.assertEqual(clusters, [int(key[0]), int(key[0]) + 10])
        self.assertEqual(sorted(keys), [(str(i),) for i in range(10)])
