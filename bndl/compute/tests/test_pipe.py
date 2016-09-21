import time

from bndl.compute.tests import DatasetTest
from bndl.rmi import InvocationException


class PipeTest(DatasetTest):
    def test_sort(self):
        lines = [
            'abc\n',
            'zxyv\n',
            'anaasdf \n',
        ]
        self.assertEqual(sorted(self.ctx.collection(lines, pcount=1).map(str.encode).pipe('sort').map(bytes.decode).collect()),
                         sorted(lines))


    def test_failure(self):
        with self.assertRaises(InvocationException):
            (self.ctx.range(3)
                     .map(lambda i: time.sleep(1) or exec('raise Exception()'))
                     .pipe('sort').collect()
            )
