from functools import partial

from bndl.compute.elastic.client import elastic_client
from bndl.compute.elastic.conf import resource_from_conf
from elasticsearch.helpers import bulk


def _refresh_index(job, name, hosts=None):
    with elastic_client(job.ctx, hosts=hosts) as client:
        client.indices.refresh(name)


def execute_bulk(ctx, actions, hosts=None):
    '''
    Perform actions on Elastic in bulk.
    
    http://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk
    provides more details on the expected format.
    
    :param ctx: bndl.compute.context.ComputeContext
        The ComputeContext to use for getting the Elastic client.
    :param actions: iterable
        An iterable of actions to execute.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    with elastic_client(ctx, hosts=hosts) as client:
        stats = bulk(client, actions, stats_only=True)
        return (stats[0],)


def elastic_bulk(self, refresh_index=None, hosts=None):
    '''
    Perform actions on Elastic in bulk.
    
    http://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk
    provides more details on the expected format.
    
    :param refresh_index: str (optional)
        Comma separated name of the indices to refresh or None to skip.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    exec_bulk = self.map_partitions(partial(execute_bulk, self.ctx, hosts=hosts))
    if refresh_index:
        exec_bulk.cleanup = partial(_refresh_index, name=refresh_index, hosts=hosts)
    return exec_bulk


def _elastic_bulk(actions, index=None, doc_type=None, refresh=False, hosts=None):
    return actions.elastic_bulk(refresh_index=index if refresh else None, hosts=hosts)


def elastic_index(self, index=None, doc_type=None, refresh=False, hosts=None):
    '''
    Index documents into Elastic.
    
    :param index: str (optional)
        Name of the index.
    :param doc_type: str (optional)
        Name of the document type.
    :param refresh: bool (optional)
        Whether to refresh the index.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    index, doc_type = resource_from_conf(self.ctx, index, doc_type)
    return _elastic_bulk(self.map(lambda doc: {
        '_op_type': 'index',
        '_index': index,
        '_type': doc_type,
        '_source': doc,
    }), index, doc_type, refresh, hosts)


def elastic_create(self, index=None, doc_type=None, refresh=False, hosts=None):
    '''
    Create documents in Elastic from a dataset of key (document id), value
    (document) pairs. Documents are only indexed if the document id doesn't
    exist yet.
    
    :param index: str (optional)
        The index name.
    :param doc_type: str (optional)
        The document type name.
    :param refresh: bool (optional)
        Whether to refresh the index afterwards.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    index, doc_type = resource_from_conf(self.ctx, index, doc_type)
    return _elastic_bulk(self.starmap(lambda doc_id, doc: {
        '_op_type': 'create',
        '_index': index,
        '_type': doc_type,
        '_id': doc_id,
        '_source': doc,
    }), index, doc_type, refresh, hosts)


def elastic_update(self, index=None, doc_type=None, refresh=False, hosts=None):
    '''
    Update documents in Elastic search from a dataset of key (document id),
    value ((partial) document) pairs.
    
    :param index: str (optional)
        The index name.
    :param doc_type: str (optional)
        The document type name.
    :param refresh: bool (optional)
        Whether to refresh the index afterwards.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    index, doc_type = resource_from_conf(self.ctx, index, doc_type)
    return _elastic_bulk(self.starmap(lambda doc_id, doc: {
        '_op_type': 'update',
        '_index': index,
        '_type': doc_type,
        '_id': doc_id,
        'doc': doc,
    }), index, doc_type, refresh, hosts)


def elastic_upsert(self, index=None, doc_type=None, refresh=False, hosts=None):
    '''
    Upsert (update or create) documents in Elastic search from a dataset of key
    (document id), value ((partial) document) pairs.
    
    :param index: str (optional)
        The index name.
    :param doc_type: str (optional)
        The document type name.
    :param refresh: bool (optional)
        Whether to refresh the index afterwards.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    index, doc_type = resource_from_conf(self.ctx, index, doc_type)
    return _elastic_bulk(self.starmap(lambda doc_id, doc: {
        '_op_type': 'update',
        '_index': index,
        '_type': doc_type,
        '_id': doc_id,
        'doc': doc,
        'doc_as_upsert': True
    }), index, doc_type, refresh, hosts)


def elastic_delete(self, index=None, doc_type=None, refresh=False, hosts=None):
    '''
    Delete documents from Elastic given their ids as dataset.
    
    :param index: str (optional)
        The index to delete the documents from.
    :param doc_type: str (optional)
        The document type to delete the documents from.
    :param refresh: bool (optional)
        Whether to refresh the index afterwards.
    :param hosts: str or iterable (optional)
        Hosts which serve as contact points for the Elastic client.
    '''
    index, doc_type = resource_from_conf(self.ctx, index, doc_type)
    return _elastic_bulk(self.map(lambda doc_id: {
        '_op_type': 'delete',
        '_index': index,
        '_type': doc_type,
        '_id': doc_id,
    }), index, doc_type, refresh, hosts)
