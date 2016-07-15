from bndl.compute.context import ComputeContext
from bndl.compute.elastic.dataset import ElasticSearchDataset
from bndl.compute.elastic.client import elastic_client
from bndl.util.funcs import as_method
from bndl.compute.dataset.base import Dataset
from bndl.compute.elastic.bulk import elastic_bulk, elastic_create, \
    elastic_index, elastic_update, elastic_delete, elastic_upsert


from bndl.util.conf import CSV, String

# Configuration
hosts = CSV()
index = String()
doc_type = String()


# Bndl API extensions
ComputeContext.elastic_client = elastic_client
ComputeContext.elastic_search = as_method(ElasticSearchDataset)

Dataset.elastic_bulk = elastic_bulk
Dataset.elastic_index = elastic_index
Dataset.elastic_create = elastic_create
Dataset.elastic_update = elastic_update
Dataset.elastic_upsert = elastic_upsert
Dataset.elastic_delete = elastic_delete
