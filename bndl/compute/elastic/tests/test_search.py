from bndl.compute.elastic.tests import ElasticTest
from functools import partial


class SearchTest(ElasticTest):
    def setUp(self):
        super().setUp()
        with self.ctx.elastic_client() as client:
            for i in range(100):
                client.index(self.index, self.doc_type, {
                    'name': str(i),
                    'number': i,
                }, id=i)

            client.indices.refresh(self.index)

    def test_search(self):
        dset = self.ctx.elastic_read(self.index, self.doc_type)
        self.assertEqual(dset.count(), 100)
        hits = dset.collect()
        ids = [hit['_id'] for hit in hits]
        names = [hit['_source']['name'] for hit in hits]
        self.assertEqual(len(set(ids)), 100)
        self.assertSequenceEqual(ids, names)

        read = partial(self.ctx.elastic_read, self.index, self.doc_type)
        self.assertEqual(read(q='10').count(), 1)
        self.assertEqual(read(query={
            'query': { 'range': { 'number': { 'gte': 20, 'lt': 50 } } }
        }).count(), 30)
