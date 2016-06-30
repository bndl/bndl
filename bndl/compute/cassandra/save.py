from collections import defaultdict
from datetime import timedelta, date, datetime
from threading import Condition
import functools
import logging

from bndl.compute.cassandra import conf
from bndl.compute.cassandra.session import cassandra_session
from bndl.util.timestamps import ms_timestamp
from cassandra import ConsistencyLevel, Unavailable, OperationTimedOut, \
    WriteTimeout, CoordinationFailure


logger = logging.getLogger(__name__)

TRANSIENT_ERRORS = (Unavailable, WriteTimeout, OperationTimedOut, CoordinationFailure)

INSERT_TEMPLATE = (
    'insert into {keyspace}.{table} '
    '({columns}) values ({placeholders})'
    '{using}'
)


def execute_save(ctx, statement, iterable, contact_points=None):
    '''
    Save elements from an iterable given the insert/update query. Use
    cassandra_save to save a dataset. This method is useful when saving
    to multiple tables from a single dataset with map_partitions.

    :param ctx: bndl.compute.context.ComputeContext
        The BNDL compute context to use for configuration and accessing the
        cassandra_session.
    :param statement: str
        The Cassandra statement to use in saving the iterable.
    :param iterable: list, tuple, generator, iterable, ...
        The values to save.
    :param contact_points: str, tuple, or list
        A string or tuple/list of strings denoting hostnames (contact points)
        of the Cassandra cluster to save to. Defaults to using the ip addresses
        in the BNDL cluster.
    :return: A count of the records saved.
    '''
    consistency_level = ctx.conf.get_attr(conf.WRITE_CONSISTENCY_LEVEL, obj=ConsistencyLevel, defaults=conf.DEFAULTS)
    timeout = ctx.conf.get_int(conf.WRITE_TIMEOUT, defaults=conf.DEFAULTS)
    retry_count = ctx.conf.get_int(conf.WRITE_RETRY_COUNT, defaults=conf.DEFAULTS)
    concurrency = max(1, ctx.conf.get_int(conf.WRITE_CONCURRENCY, defaults=conf.DEFAULTS))

    if logger.isEnabledFor(logging.INFO):
        logger.info('executing cassandra save with statement %s', statement.replace('\n', ''))

    with cassandra_session(ctx, contact_points=contact_points) as session:
        prepared_statement = session.prepare(statement)
        prepared_statement.consistency_level = consistency_level

        saved = 0
        pending = 0
        cond = Condition()

        failure = None
        failcounts = defaultdict(int)

        def on_done(results, idx):
            nonlocal saved, pending, cond
            with cond:
                saved += 1
                pending -= 1
                cond.notify_all()

        def on_failed(exc, idx, element):
            nonlocal failcounts
            if exc in TRANSIENT_ERRORS and failcounts[idx] < retry_count:
                failcounts[idx] += 1
                exec_async(idx, element)
            else:
                nonlocal failure, pending, cond
                with cond:
                    failure = exc
                    pending -= 1
                    cond.notify_all()

        def exec_async(idx, element):
            future = session.execute_async(prepared_statement, element, timeout=timeout)
            future.add_callback(on_done, idx)
            future.add_errback(on_failed, idx, element)

        for idx, element in enumerate(iterable):
            with cond:
                if failure:
                    raise failure
                cond.wait_for(lambda: pending < concurrency)
                pending += 1
            exec_async(idx, element)

        if failure:
            raise failure

        with cond:
            cond.wait_for(lambda: pending == 0)

        if failure:
            raise failure

    return (saved,)


def cassandra_save(dataset, keyspace, table, columns=None, keyed_rows=True,
                   ttl=None, timestamp=None, contact_points=None):
    '''
    Performs a Cassandra insert for each element of the dataset.

    :param dataset: bndl.compute.context.ComputationContext
        As the cassandra_save function is typically bound on ComputationContext
        this corresponds to self.
    :param keyspace: str
        The name of the Cassandra keyspace to save to.
    :param table:
        The name of the Cassandra table (column family) to save to.
    :param columns:
        The names of the columns to save.

        When the dataset contains tuples, this arguments is the mapping of the
        tuple elements to the columns of the table saved to. If not provided,
        all columns are used.

        When the dataset contains dictionaries, this parameter limits the
        columns save to Cassandra.
    :param keyed_rows: bool
        Whether to expect a dataset of dicts (or at least supports the
        __getitem__ protocol with columns names as key) or positional objects
        (e.g. tuples or lists).
    :param ttl: int
        The time to live to use in saving the records.
    :param timestamp: int or datetime.date or datetime.datetime
        The timestamp to use in saving the records.
    :param contact_points: str, tuple, or list
        A string or tuple/list of strings denoting hostnames (contact points)
        of the Cassandra cluster to save to. Defaults to using the ip addresses
        in the BNDL cluster.

    Example:

        >>> ctx.collection([{'key': 1, 'val': 'a' }, {'key': 2, 'val': 'b' },]) \
               .cassandra_save('keyspace', 'table') \
               .execute()
        >>> ctx.range(100) \
               .map(lambda i: {'key': i, 'val': str(i) } \
               .cassandra_save('keyspace', 'table') \
               .sum()
        100
    '''
    if ttl or timestamp:
        using = []
        if ttl:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds() * 1000)
            using.append('ttl ' + str(ttl))
        if timestamp:
            if isinstance(timestamp, (date, datetime)):
                timestamp = ms_timestamp(timestamp)
            using.append('timestamp ' + str(timestamp))
        using = ' using ' + ' and '.join(using)
    else:
        using = ''

    if not columns:
        with dataset.ctx.cassandra_session(contact_points=contact_points) as session:
            table_meta = session.cluster.metadata.keyspaces[keyspace].tables[table]
            columns = list(table_meta.columns)

    placeholders = (','.join(
        (':' + c for c in columns)
        if keyed_rows else
        ('?' for c in columns)
    ))

    insert = INSERT_TEMPLATE.format(
        keyspace=keyspace,
        table=table,
        columns=', '.join(columns),
        placeholders=placeholders,
        using=using,
    )

    do_save = functools.partial(execute_save, dataset.ctx, insert, contact_points=contact_points)
    return dataset.map_partitions(do_save)
