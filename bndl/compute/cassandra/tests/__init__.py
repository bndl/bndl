from bndl.compute import cassandra  # @UnusedImport
from bndl.compute.dataset.tests import DatasetTest


class CassandraTest(DatasetTest):
    keyspace = 'bndl_cassandra_test'
    table = 'test_table'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with cls.ctx.cassandra_session() as s:
            s.execute('''
                create keyspace if not exists {keyspace} 
                with replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': '1'
                }};
            '''.format(keyspace=cls.keyspace))

            s.execute('''
                create table if not exists {keyspace}.{table} (
                    key text,
                    cluster text,
                    int_list list<int>,
                    double_set set<double>,
                    text_map map<text,text>,
                    timestamp_val timestamp,
                    varint_val varint,
                    primary key (key, cluster)
                );
            '''.format(keyspace=cls.keyspace, table=cls.table))


    def setUp(self):
        super().setUp()
        with self.ctx.cassandra_session() as s:
            s.execute('truncate {keyspace}.{table};'.format(keyspace=self.keyspace, table=self.table))
