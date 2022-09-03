# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import numpy as np


def has_constant(x):
    """
    Natural dataset cannot has a constant column spontaneously.
    So if coefficient matrix already has one or more constant columns,
    adding a constant column will not affect the rank of the matrix.
    Otherwise, the added column will add 1 to the rank of the matrxix.
    So we use this feature to check if the coefficient matrix has
    user-defined constant column
    """

    if x.size == 0:
        return False

    origin_rank = np.linalg.matrix_rank(x)
    augmented_x = np.column_stack((np.ones(x.shape[0]), x))
    augmented_rank = np.linalg.matrix_rank(augmented_x)
    has_constant_column = int(origin_rank == augmented_rank)
    return has_constant_column


def uniform_data(x):
    x = np.array(x)
    if x.ndim == 1:
        x = x[:, np.newaxis]

    if x.ndim != 2:
        raise ValueError("x must be 2D array")

    return x


class OLS:
    """
    Ordinary Least Squares
    The OLS can calculate the least square solution to the equations
    OLS will not consider the situation that endog-x has constant
    columns by default.
    """

    def __init__(self, x, y):
        self._x = uniform_data(x)  # x.shape is (self.length, nlags)
        self._y = uniform_data(y).flatten()  # y.shape is (self.length, 1)
        self.length = float(self._x.shape[0])
        self.has_constant = has_constant(self._x)
        self.rank = None
        self.coefficient_matrix = None
        self.dof_model = None
        self.dof_resid = None
        self.normalized_cov_params = None

    @property
    def resid(self):
        """
        The residuals of the regressand and regressor(s).
        """

        return self._y - self.predict(self.coefficient_matrix, self._x)

    def loglike(self, coefficient_matrix=None):
        """
        The likelihood function for the OLS model.
        """

        if coefficient_matrix is None:
            resid = self.resid
        else:
            resid = self._y - self.predict(coefficient_matrix, self._x)

        sigma2 = np.sum(resid ** 2) / float(self.length)
        llf = -self.length * (np.log(2 * np.pi * sigma2) + 1) / 2.0
        return llf

    @property
    def bic(self):
        """
        Bayesian Information Criterion
        For a model with a constant : -2llf + ln(n)(dof_model+1).
        For a model without a constant : -2llf + ln(n)(dof_model).
        """

        llf = self.loglike(self.coefficient_matrix)
        return -2 * llf + np.log(self.length) * (self.dof_model + self.has_constant)

    @property
    def scale(self):
        """
        a scale factor for the covariance matrix.
        """

        resid = self.resid
        return np.dot(resid, resid) / self.dof_resid

    def cov_params(self):
        """
        Compute the variance/covariance matrix.
        """

        return self.normalized_cov_params * self.scale

    @property
    def tvalues(self):
        """
        return the t-statistic for a given parameter estimate.
        """

        return self.coefficient_matrix / np.sqrt(np.diag(self.cov_params()))

    def fit(self):
        """
        The fit method uses the pseudo inverse matrix to calculate
        the least square solution 'C' to the equation
        y = x·C  --> C = pinv_x·y
        """

        x_inverse = np.linalg.pinv(self._x)
        x_singular = np.linalg.svd(self._x, False)[1]
        self.rank = np.linalg.matrix_rank(np.diag(x_singular)) if self._x.size else 0
        self.coefficient_matrix = np.dot(x_inverse, self._y)
        self.dof_model = float(self.rank - self.has_constant)
        self.dof_resid = self.length - self.rank
        self.normalized_cov_params = np.dot(x_inverse, np.transpose(x_inverse))
        return self.coefficient_matrix

    def predict(self, coefficient_matrix, x=None):
        if x is None:
            x = self._x
        return np.dot(x, coefficient_matrix)
