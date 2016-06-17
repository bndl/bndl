from bndl.compute.elastic.tests import ElasticTest
from bndl.rmi.invocation import InvocationException


class IndexTest(ElasticTest):
    def test_index(self):
        # create
        inserts = self.ctx.range(100).with_value(lambda i: {'name': str(i)})
        saved = inserts.elastic_create(refresh=True).sum()
        self.assertEqual(saved, 100)
        scan = self.ctx.elastic_read()
        self.assertEqual(scan.count(), 100)

        # update
        updates = self.ctx.range(100).with_value(lambda i: {'number': i})
        updated = updates.elastic_update(refresh=True).sum()
        self.assertEqual(updated, 100)
        self.assertEqual(scan.count(), 100)
        hits = scan.collect()
        self.assertEqual(list(range(100)), sorted(int(hit['_id']) for hit in hits))
        self.assertEqual(list(range(100)), sorted(int(hit['_source']['name']) for hit in hits))
        self.assertEqual(list(range(100)), sorted(hit['_source']['number'] for hit in hits))

        # upsert
        upserts = self.ctx.range(200).with_value(lambda i: {'text': str(i)})
        upserted = upserts.elastic_upsert(refresh=True).sum()
        self.assertEqual(upserted, 200)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'name' in hit).count(), 100)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'number' in hit).count(), 100)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'text' in hit).count(), 200)
        self.assertEqual(scan.pluck('_id').map(int).sort().collect(), list(range(200)))

        # create again
        creates = self.ctx.range(200, 300).with_value(lambda i: {'text': str(i)})
        created = creates.elastic_create(refresh=True).sum()
        self.assertEqual(created, 100)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'name' in hit).count(), 100)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'number' in hit).count(), 100)
        self.assertEqual(scan.pluck('_source').filter(lambda hit: 'text' in hit).count(), 300)
        self.assertEqual(scan.pluck('_id').map(int).sort().collect(), list(range(300)))

        # create failure
        with self.assertRaises(InvocationException):
            self.ctx.range(300).with_value({'text':'x'}).elastic_create().execute()

        # delete
        deleted = self.ctx.range(100, 200).elastic_delete(refresh=True).sum()
        self.assertEqual(deleted, 100)
        self.assertEqual(scan.pluck('_id').map(int).sort().collect(),
                         list(range(100)) + list(range(200, 300)))
        scan.pluck('_id').elastic_delete(refresh=True).execute()
        self.assertEqual(scan.count(), 0)
