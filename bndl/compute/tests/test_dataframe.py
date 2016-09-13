from bndl.compute.tests import DatasetTest
import pandas as pd
import numpy as np
import tempfile
import csv


class DataFrameTest(DatasetTest):
    df_size = 100

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.df = cls.ctx.range(cls.df_size).map(lambda i: dict(num=i, txt=str(i))).as_dataframe()

    def test_from_range(self):
        df = self.df.collect()
        self.assertTrue((df['num'] == np.arange(100)).all())
        self.assertTrue((df['txt'] == list(map(str, range(100)))).all())
        self.assertTrue((self.df['num'].collect() == list(range(self.df_size))).all())

    def test_assign(self):
        nums2 = self.df['num'].map(lambda i: i ** 2)
        with_nums2 = self.df.assign('num2', nums2).collect()

        self.assertIn('num', with_nums2)
        self.assertIn('num2', with_nums2)

        self.assertTrue((with_nums2['num'] == np.arange(100)).all())
        self.assertTrue((with_nums2['num2'] == np.arange(100) ** 2).all())

    def test_csv(self):
        n_files = 10
        n_rows_per_file = 100
        n_rows = n_rows_per_file * n_files
        files = [tempfile.NamedTemporaryFile('w+t')
                 for _ in range(n_files)]
        for offset, file in enumerate(files):
            offset *= n_rows_per_file
            writer = csv.writer(file.file)
            rows = [[i, i ** 2, i // 2] for i in range(offset, offset + n_rows_per_file)]
            writer.writerow(list('abc'))
            for row in rows:
                writer.writerow(row)
            file.file.flush()

        df = self.ctx.files([file.name for file in files],
                             psize_bytes=1024, psize_files=3).parse_csv().collect()

        expected = pd.DataFrame(dict(
            a=np.arange(n_rows),
            b=np.arange(n_rows) ** 2,
            c=np.arange(n_rows) // 2,
        ))

        self.assertTrue((df == expected).all().all())

        for file in files:
            file.close()
