from functools import partial

from sklearn import svm, datasets
from sklearn.grid_search import GridSearchCV as SKGridSearchCV

from bndl.compute.script import ctx
from bndl.ml.grid_search import GridSearchCV as BndlGridSearchCV
import numpy as np


iris = datasets.load_iris()
parameters = {
    'kernel': ('linear', 'rbf', 'poly', 'sigmoid'),
    'C': np.arange(.1, 10, .01)
}

svr = svm.SVC()

for gsearch in (partial(SKGridSearchCV, n_jobs=4), partial(BndlGridSearchCV, ctx)):
    clf = gsearch(estimator=svr, param_grid=parameters)
    clf.fit(iris.data, iris.target)
    print(sum(1 for e in clf.predict(iris.data) == iris.target if e), '/', len(iris.data))
