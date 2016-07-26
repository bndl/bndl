from bndl.compute.cassandra.dataset import CassandraScanDataset
from bndl.compute.cassandra.save import cassandra_save
from bndl.compute.cassandra.session import cassandra_session
from bndl.compute.context import ComputeContext
from bndl.compute.dataset.base import Dataset
from bndl.util.conf import Int, Float, CSV, String, Bool, Attr
from bndl.util.funcs import as_method
from cassandra import ConsistencyLevel

# Configuration
contact_points = CSV()
port = Int(9042)
keyspace = String()
compression = Bool(True)
metrics_enabled = Bool(True)

read_retry_count = Int(10)
read_retry_backoff = Float(2, desc='delay = {read_timeout_backoff} ^ retry_round - 1')
read_timeout = Int(120)
read_consistency_level = Attr(ConsistencyLevel.LOCAL_ONE, obj=ConsistencyLevel)
fetch_size_rows = Int(1000)
part_size_keys = Int(1000 * 1000)
part_size_mb = Int(64)

write_retry_count = Int(0)
write_retry_backoff = Float(2, desc='delay = {write_timeout_backoff} ^ retry_round - 1')
write_timeout = Int(120)
write_concurrency = Int(5)
write_consistency_level = Attr(ConsistencyLevel.LOCAL_QUORUM, obj=ConsistencyLevel)


# Bndl API extensions
ComputeContext.cassandra_session = cassandra_session
ComputeContext.cassandra_table = as_method(CassandraScanDataset)
Dataset.cassandra_save = cassandra_save
