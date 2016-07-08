CONTACT_POINTS = 'cassandra.contact_points'
PORT = 'cassandra.port'
KEYSPACE = 'cassandra.keyspace'
COMPRESSION = 'cassandra.compression'
METRICS_ENABLED = 'cassandra.metrics_enabled'

READ_RETRY_COUNT = 'cassandra.read_retry_count'
READ_TIMEOUT = 'cassandra.read_timeout'
READ_CONSISTENCY_LEVEL = 'cassandra.read_consistency_level'
FETCH_SIZE_ROWS = 'cassandra.fetch_size_rows'
PART_SIZE_KEYS = 'cassandra.read_part_size_keys'
PART_SIZE_MB = 'cassandra.read_part_size_mb'


WRITE_RETRY_COUNT = 'cassandra.write_retry_count'
WRITE_TIMEOUT = 'cassandra.write_retry_count'
WRITE_CONCURRENCY = 'cassandra.write_concurrency'
WRITE_CONSISTENCY_LEVEL = 'cassandra.write_consistency_level'


DEFAULTS = {
    CONTACT_POINTS: None,
    PORT: 9042,
    KEYSPACE: None,
    COMPRESSION: True,
    METRICS_ENABLED: True,

    READ_RETRY_COUNT: 10,
    READ_TIMEOUT: 120,
    READ_CONSISTENCY_LEVEL: 'LOCAL_ONE',
    FETCH_SIZE_ROWS: 1000,
    PART_SIZE_KEYS: 1000 * 1000,
    PART_SIZE_MB: 64,

    WRITE_RETRY_COUNT: 0,
    WRITE_TIMEOUT: 120,
    WRITE_CONCURRENCY: 5,
    WRITE_CONSISTENCY_LEVEL: 'LOCAL_QUORUM',
}
