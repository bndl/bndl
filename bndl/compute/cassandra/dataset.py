from bndl.compute.cassandra import partitioner
from bndl.compute.cassandra.session import cassandra_session
from bndl.compute.dataset.base import Dataset, Partition
from bndl.util import collection
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import tuple_factory, named_tuple_factory, dict_factory


class CassandraScanDataset(Dataset):
    def __init__(self, ctx, keyspace, table):
        super().__init__(ctx)
        self.keyspace = keyspace
        self.table = table
        self._row_factory = named_tuple_factory

        with ctx.cassandra_session() as session:
            keyspace_meta = session.cluster.metadata.keyspaces[self.keyspace]
            table_meta = keyspace_meta.tables[self.table]

        self._select = '*'
        self._limit = None
        self._where = '''
            token({partition_key_column_names}) > ? and
            token({partition_key_column_names}) <= ?
        '''.format(
            partition_key_column_names=', '.join(c.name for c in table_meta.partition_key)
        )


    def count(self, push_down=True):
        if push_down or not self.cached:
            return self.select('count(*)').as_tuples().map(collection.getter(0)).sum()
        else:
            return super().count()


    def as_tuples(self):
        return self._with('_row_factory', tuple_factory)

    def as_dicts(self):
        return self._with('_row_factory', dict_factory)

    def select(self, *columns):
        return self._with('_select', ', '.join(columns))


    def limit(self, num):
        return self._with('_limit', int(num))

    def itake(self, num):
        if not self.cached and not self._limit:
            return self.limit(num).itake(num)
        else:
            return super().itake(num)


    def parts(self):
        # TODO assign in a node local fashion
        # combining token ranges into partitions if they fit
        # spreading across nodes with node locality preferred
        with cassandra_session(self.ctx) as session:
            partitions = partitioner.partition_ranges(session, self.keyspace, self.table)
            # TODO if len(partitions) < self._ctx.default_part_count:

        # print('creating', len(partitions), 'partitions')
        return [
            CassandraScanPartition(self, i, replicas, token_ranges)
            for i, (replicas, token_ranges) in enumerate(partitions)
        ]


    def query(self, session):
        limit = ' limit %s' % self._limit if self._limit else ''
        query = '''
            select {select}
            from {keyspace}.{table}
            where {where}{limit}
        '''.format(
            select=self._select,
            keyspace=self.keyspace,
            table=self.table,
            where=self._where,
            limit=limit
        )
        return session.prepare(query)


class CassandraScanPartition(Partition):
    def __init__(self, dset, part_idx, replicas, token_ranges):
        super().__init__(dset, part_idx)
        self.replicas = replicas
        self.token_ranges = token_ranges


    def _materialize(self, ctx):
        # print('materializing cassandra scan partition', self.idx)
        # print('scanning', self.token_range, 'in process', os.getpid())
        with ctx.cassandra_session() as session:
            session.row_factory = named_tuple_factory
            # session.default_fetch_size = 1000
            # session.client_protocol_handler = NumpyProtocolHandler
            session.row_factory = self.dset._row_factory
            query = self.dset.query(session)

            results = execute_concurrent_with_args(session, query, self.token_ranges, concurrency=64, results_generator=True)
            for success, rows in results:
                assert success  # TODO handle failure
                for row in rows:
                    yield row

    def preferred_workers(self, workers):
        return [
            worker
            for worker in workers
            for replica in self.replicas
            if replica.address in worker.ip_addresses
        ]