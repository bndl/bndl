from bndl.compute.tests import DatasetTest
import numpy as np


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
