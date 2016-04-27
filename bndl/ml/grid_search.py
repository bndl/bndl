from sklearn.base import is_classifier, clone
from sklearn.cross_validation import check_cv, _fit_and_score
from sklearn.grid_search import GridSearchCV as SKGridSearchCV, _CVScoreTuple
from sklearn.metrics.scorer import check_scoring
from sklearn.utils.validation import _num_samples, indexable

import numpy as np


class GridSearchCV(SKGridSearchCV):
    def __init__(self, ctx, estimator, param_grid, scoring=None, fit_params=None,
                 iid=True, refit=True, cv=None, verbose=0, error_score='raise'):
        super(GridSearchCV, self).__init__(
            estimator, param_grid, scoring=scoring, fit_params=fit_params, iid=iid,
            refit=refit, cv=cv, verbose=verbose, error_score=error_score)
        self.ctx = ctx

    def _fit(self, X, y, parameter_iterable):
        estimator = self.estimator
        self.scorer_ = check_scoring(self.estimator, scoring=self.scoring)

        n_samples = _num_samples(X)
        X, y = indexable(X, y)

        if y is not None:
            if len(y) != n_samples:
                raise ValueError('Target variable (y) has a different number '
                                 'of samples (%i) than data (X: %i samples)'
                                 % (len(y), n_samples))



        base_estimator = clone(self.estimator)
        cv = check_cv(self.cv, X, y, classifier=is_classifier(estimator))
        n_folds = len(cv)

        Xycv = self.ctx.broadcast((X, y, cv))

        try:
            def fit_and_score(parameters):
                X, y, cv = Xycv.value
                return parameters, [_fit_and_score(clone(base_estimator), X, y, self.scorer_,
                                        train, test, self.verbose, parameters,
                                        self.fit_params, error_score=self.error_score)
                                    for train, test in cv]

            scores = self.ctx.collection(parameter_iterable, pcount=len(parameter_iterable)).map(fit_and_score).collect()
        finally:
            Xycv.unpersist()

        grid_scores = []

        for parameters, fold_scores in scores:
            n_test_samples = 0
            score = 0
            all_scores = []
            for this_score, this_n_test_samples, _ in fold_scores:
                all_scores.append(this_score)
                if self.iid:
                    this_score *= this_n_test_samples
                    n_test_samples += this_n_test_samples
                score += this_score
            if self.iid:
                score /= float(n_test_samples)
            else:
                score /= float(n_folds)

            grid_scores.append(_CVScoreTuple(
                parameters,
                score,
                np.array(all_scores)))

        self.grid_scores_ = grid_scores
        best = sorted(grid_scores, key=lambda x: x.mean_validation_score,
                      reverse=True)[0]
        self.best_params_ = best.parameters
        self.best_score_ = best.mean_validation_score
        if self.refit:
            # fit the best estimator using the entire dataset
            # clone first to work around broken estimators
            best_estimator = clone(base_estimator).set_params(**best.parameters)
            if y is not None:
                best_estimator.fit(X, y, **self.fit_params)
            else:
                best_estimator.fit(X, **self.fit_params)
            self.best_estimator_ = best_estimator
        return self
