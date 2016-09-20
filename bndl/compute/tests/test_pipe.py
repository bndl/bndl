from bndl.compute.tests import DatasetTest


class PipeTest(DatasetTest):
    def test_sort(self):
        lines = [
            'abc\n',
            'zxyv\n',
            'anaasdf \n',
        ]
        self.assertEqual(sorted(self.ctx.collection(lines, pcount=1).map(str.encode).pipe('sort').map(bytes.decode).collect()),
                         sorted(lines))
