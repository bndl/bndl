from sklearn import svm, datasets
from sklearn.grid_search import GridSearchCV as SKGridSearchCV

from bndl.compute.tests import ComputeTest
from bndl.ml.grid_search import GridSearchCV as BndlGridSearchCV
import numpy as np


class GridSearchTestCase(ComputeTest):
    def test_svm_iris(self):
        iris = datasets.load_iris()
        parameters = {
            'kernel': ('linear', 'rbf', 'poly', 'sigmoid'),
            'C': np.arange(.1, 10, .1)
        }

        sk_clf = SKGridSearchCV(svm.SVC(), parameters, n_jobs=-1)
        bndl_clf = BndlGridSearchCV(self.ctx, svm.SVC(), parameters)

        for clf in (bndl_clf, sk_clf):
            clf.fit(iris.data, iris.target)

        self.assertDictEqual(sk_clf.best_params_, bndl_clf.best_params_)
