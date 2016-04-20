from datetime import timedelta, date, datetime
import functools
import logging

from bndl.compute.cassandra.session import cassandra_session
from bndl.util.timestamps import ms_timestamp
from cassandra.concurrent import execute_concurrent_with_args


logger = logging.getLogger(__name__)


insert_template = (
'insert into {keyspace}.{table} '
'({columns}) values ({placeholders})'
'{using}'
)


def _save_part(insert, concurrency, part, iterable, contact_points=None):
    logger.info('executing cassandra save on part %s with insert %s', part.idx, insert.replace('\n', ''))
    with cassandra_session(part.dset.ctx, contact_points=contact_points) as session:
        prepared_insert = session.prepare(insert)
        results = execute_concurrent_with_args(session, prepared_insert, iterable, concurrency=concurrency)
        return [len(results)]


def cassandra_save(dataset, keyspace, table, columns=None, keyed_rows=True, ttl=None, timestamp=None, concurrency=10, contact_points=None):
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

    insert = insert_template.format(
        keyspace=keyspace,
        table=table,
        columns=', '.join(columns),
        placeholders=placeholders,
        using=using,
    )

    return dataset.map_partitions_with_part(functools.partial(_save_part, insert, concurrency, contact_points=contact_points))
