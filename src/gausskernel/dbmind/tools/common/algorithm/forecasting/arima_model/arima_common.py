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

from types import SimpleNamespace
import numpy as np


def _right_squeeze(arr, stop_dim=0):
    """
    remove trailing singleton dimensions
    :param arr: type->np.array
    :param stop_dim: type->int
    :return: arr: type->np.array
    """
    last = arr.ndim
    for squeeze in reversed(arr.shape):
        if squeeze > 1:
            break
        last -= 1
    last = max(last, stop_dim)

    return arr.reshape(arr.shape[:last])


def array_like(
    obj,
    name,
    ndim=1,
    order=None,
    optional=False,
):
    """
    Convert array-like to a ndarray and check conditions
    """
    if optional and obj is None:
        return None
    arr = np.asarray(obj, dtype=None, order=order)
    if ndim is not None:
        if arr.ndim > ndim:
            arr = _right_squeeze(arr, stop_dim=ndim)
        elif arr.ndim < ndim:
            arr = np.reshape(arr, arr.shape + (1,) * (ndim - arr.ndim))
        if arr.ndim != ndim:
            msg = "{0} is required to have ndim {1} but has ndim {2}"
            raise ValueError(msg.format(name, ndim, arr.ndim))
    return arr


def lagmat(x_raw, maxlag, trim='forward', original='ex'):
    """
    create 2d array of lags.
    """

    x_raw = array_like(x_raw, 'x', ndim=2)
    trim = 'none' if trim is None else trim
    trim = trim.lower()

    dropidx = 0
    nobs, nvar = x_raw.shape
    if original in ['ex', 'sep']:
        dropidx = nvar
    if maxlag >= nobs:
        raise ValueError("maxlag should be < nobs")
    lmat = np.zeros((nobs + maxlag, nvar * (maxlag + 1)))
    for k in range(0, int(maxlag + 1)):
        lmat[maxlag - k:nobs + maxlag - k,
        nvar * (maxlag - k):nvar * (maxlag - k + 1)] = x_raw

    if trim in ('none', 'forward'):
        startobs = 0
    elif trim in ('backward', 'both'):
        startobs = maxlag
    else:
        raise ValueError('trim option not valid')

    if trim in ('none', 'backward'):
        stopobs = len(lmat)
    else:
        stopobs = nobs

    lags = lmat[startobs:stopobs, dropidx:]
    if original == 'sep':
        leads = lmat[startobs:stopobs, :dropidx]

    if original == 'sep':
        return lags, leads

    return lags


def get_matrix_inverse_and_singular(x_raw, rcond=1e-15):
    """
    (1)the linear equation: 'Y = AX', so 'A = np.dot(inv_X, Y)',
    this function to return inv_X;
    (2)use SVD decompose to compute X_inv, like: X = USV_t, then
    X_inv = np.dot(V, np.dot(inv_S, U_t))
    (3)return singular values of X from SVD decompose
    """
    x_raw = np.asarray(x_raw)
    u_mat, s_mat, vt_mat = np.linalg.svd(x_raw, False)
    raw_s = np.copy(s_mat)
    rank = min(u_mat.shape[0], vt_mat.shape[1])
    cutoff_value = rcond * np.maximum.reduce(s_mat)

    inv_s = np.asarray([1./s_mat[i] if s_mat[i] > cutoff_value else 0 for i in range(rank)])

    inv_x = np.dot(np.transpose(vt_mat),
                   np.multiply(inv_s[:, np.core.newaxis], np.transpose(u_mat)))

    return inv_x, raw_s


def get_k_constant(x_raw):
    """return the k_constant of x matrix"""
    augmented_x = np.column_stack((np.ones(x_raw.shape[0]), x_raw))
    augm_rank = np.linalg.matrix_rank(augmented_x)
    orig_rank = np.linalg.matrix_rank(x_raw)
    k_constant = int(orig_rank == augm_rank)
    return k_constant


class OLS():
    """The OLS can compute linear correlation coefficient about x and y"""
    def __init__(self, y_raw, x_raw):
        self._x = np.asarray(x_raw)
        self._y = np.asarray(y_raw.flatten())
        self.nobs = float(self._x.shape[0])
        self.matrix_param = SimpleNamespace(df_model=None,
                                            df_resid=None,
                                            rank=None,
                                            k_constant=get_k_constant(self._x))
        self.coeffs = None
        self.normalized_cov_params = None

    @property
    def df_model(self):
        """
        the model degree of freedom.
        """
        return self.matrix_param.df_model


    @property
    def df_resid(self):
        """
        the residual degree of freedom.
        """
        return self.matrix_param.df_resid

    @df_resid.setter
    def df_resid(self, value):
        self.matrix_param.df_resid = value

    @property
    def wresid(self):
        """
        The residuals of the transformed/whitened regressand and regressor(s).
        """
        return self._y - self.predict(self.coeffs, self._x)

    @property
    def scale(self):
        """
        a scale factor for the covariance matrix.
        """
        wresid = self.wresid
        return np.dot(wresid, wresid) / self.df_resid

    @property
    def tvalues(self):
        """
        return the t-statistic for a given parameter estimate.
        """
        return self.coeffs / np.sqrt(np.diag(self.cov_params()))

    def cov_params(self):
        """
        Compute the variance/covariance matrix.
        """
        return self.normalized_cov_params * self.scale

    @property
    def llf(self):
        """the llf for errors estimated is used to compute BIC"""
        return self.loglike(self.coeffs)

    def loglike(self, coeffs):
        """
        The likelihood function for the OLS model.
        """
        nobs2 = self.nobs / 2.0
        nobs = float(self.nobs)
        resid = np.array(self._y) - np.dot(self._x, coeffs)
        ssr = np.sum(resid**2)
        llf = -nobs2*np.log(2*np.pi) - nobs2*np.log(ssr / nobs) - nobs2

        return llf

    @property
    def bic(self):
        r"""
        For a model with a constant :math:`-2llf + \log(n)(df\_model+1)`.
        For a model without a constant :math:`-2llf + \log(n)(df\_model)`.
        """
        return (-2 * self.llf + np.log(self.nobs) * (self.df_model +
                                                     self.matrix_param.k_constant))

    def fit(self):
        """
        full fit of the model.
        """
        x_inverse, x_singular = get_matrix_inverse_and_singular(self._x)
        self.normalized_cov_params = np.dot(x_inverse, np.transpose(x_inverse))

        self.matrix_param.rank = np.linalg.matrix_rank(np.diag(x_singular))
        self.coeffs = np.dot(x_inverse, self._y)

        if self.matrix_param.df_model is None:
            self.matrix_param.df_model = float(self.matrix_param.rank -
                                               self.matrix_param.k_constant)
        if self.matrix_param.df_resid is None:
            self.matrix_param.df_resid = self.nobs - self.matrix_param.rank

        return self.coeffs

    @staticmethod
    def predict(coeffs, x_raw):
        """
        linear predict with coeffs matrix, and return predicted values.
        """
        return np.dot(x_raw, coeffs)
