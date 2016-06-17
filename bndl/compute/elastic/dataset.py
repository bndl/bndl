from bndl.compute.dataset.base import Dataset, Partition
from bndl.compute.elastic.client import elastic_client, parse_hostname
from elasticsearch.helpers import scan
from bndl.compute.elastic.conf import resource_from_conf


class ElasticSearchDataset(Dataset):
    '''
    Dataset from an Elastic index.
    
    http://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan
    is used under the covers but scanned on a per shard basis to get data
    locality.
    '''

    def __init__(self, ctx, index=None, doc_type=None, hosts=None, **kwargs):
        '''
        Create a dataset from an Elastic index.
        
        http://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan
        is used under the covers. You can expect the same arguments to work;
        provide as kwargs.
        
        :param ctx: bndl.compute.context.ComputeContext
            Can be ignored when using ComputeContext.elastic_search(...)
        :param index: str (optional)
            The index name.
        :param doc_type: str (optional)
            The document type name.
        :param hosts: str or iterable (optional)
            Hosts which serve as contact points for the Elastic client.
        :param **kwargs: dict
            Keyword arguments passed to elasticsearch.helpers.scan.
        '''
        super().__init__(ctx)
        index, doc_type = resource_from_conf(ctx, index, doc_type)
        self.index = index
        self.doc_type = doc_type
        self.hosts = hosts
        self.kwargs = kwargs

    def parts(self):
        with elastic_client(self.ctx, hosts=self.hosts) as client:
            resp = client.search_shards(self.index, self.doc_type)

        nodes = resp['nodes']
        allocations = [
            [parse_hostname(nodes[allocation['node']]['transport_address']) for allocation in shard]
            for shard in resp['shards']
        ]

        return [
            ElasticSearchScrollPartition(self, i, allocation)
            for i, allocation in enumerate(allocations)
        ]


class ElasticSearchScrollPartition(Partition):
    def __init__(self, dset, idx, allocation):
        super().__init__(dset, idx)
        self.allocation = set(allocation)

    def _preferred_workers(self, workers):
        return [
            worker
            for worker in workers
            if worker.ip_addresses & self.allocation
        ]

    def _materialize(self, ctx):
        with elastic_client(ctx, hosts=self.dset.hosts) as client:
            yield from scan(
                client,
                index=self.dset.index,
                doc_type=self.dset.doc_type,
                preference='_shards:%s' % self.idx,
                **self.dset.kwargs
            )
