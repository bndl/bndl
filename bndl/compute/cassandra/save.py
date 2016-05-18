from datetime import timedelta, date, datetime
import functools
import logging
from cassandra.concurrent import execute_concurrent_with_args

from bndl.compute.cassandra.session import cassandra_session
from bndl.util.timestamps import ms_timestamp
from bndl.compute.cassandra import conf
from cassandra import ConsistencyLevel


logger = logging.getLogger(__name__)


INSERT_TEMPLATE = (
    'insert into {keyspace}.{table} '
    '({columns}) values ({placeholders})'
    '{using}'
)


def _save_part(insert, part, iterable, contact_points=None):
    if logger.isEnabledFor(logging.INFO):
        logger.info('executing cassandra save on part %s with insert %s', part.idx, insert.replace('\n', ''))
    with cassandra_session(part.dset.ctx, contact_points=contact_points) as session:
        session.default_timeout = part.dset.ctx.conf.get_int(conf.WRITE_TIMEOUT, defaults=conf.DEFAULTS)
        prepared_insert = session.prepare(insert)
        prepared_insert.consistency_level = part.dset.ctx.conf.get_attr(conf.WRITE_CONSISTENCY_LEVEL, obj=ConsistencyLevel, defaults=conf.DEFAULTS)
        concurrency = part.dset.ctx.conf.get_int(conf.WRITE_CONCURRENCY, defaults=conf.DEFAULTS)
        results = execute_concurrent_with_args(session, prepared_insert, iterable, concurrency=concurrency)
    return [len(results)]


def cassandra_save(dataset, keyspace, table, columns=None, keyed_rows=True,
                   ttl=None, timestamp=None, contact_points=None):
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

    do_save = functools.partial(_save_part, insert, contact_points=contact_points)
    return dataset.map_partitions_with_part(do_save)
