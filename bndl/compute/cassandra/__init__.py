from bndl.compute.cassandra.dataset import CassandraScanDataset
from bndl.compute.cassandra.loadbalancing import LocalNodeFirstPolicy
from bndl.compute.cassandra.save import cassandra_save
from bndl.compute.cassandra.session import cassandra_session
from bndl.compute.context import ComputeContext
from bndl.compute.dataset.base import Dataset
from bndl.util.funcs import as_method
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy


ComputeContext.cassandra_session = cassandra_session
ComputeContext.cassandra_table = as_method(CassandraScanDataset)
Dataset.cassandra_save = cassandra_save
