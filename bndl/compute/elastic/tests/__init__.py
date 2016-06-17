from bndl.compute.dataset.tests import DatasetTest
from elasticsearch.client import Elasticsearch
from bndl.compute.elastic import conf


class ElasticTest(DatasetTest):
    index = 'bndl_elastic_test'
    doc_type = 'test_doctype'

    def setUp(self):
        super().setUp()
        self.ctx.conf[conf.INDEX] = self.index
        self.ctx.conf[conf.DOC_TYPE] = self.doc_type

        with self.ctx.elastic_client() as client:
            client.indices.delete(self.index, ignore=404)
            client.indices.create(self.index, body=dict(settings=dict(index=dict(number_of_replicas=0))))
            client.indices.refresh(self.index)

    def tearDown(self):
        super().tearDown()
#         with self.ctx.elastic_client() as client:
#             client.indices.delete(self.index)
