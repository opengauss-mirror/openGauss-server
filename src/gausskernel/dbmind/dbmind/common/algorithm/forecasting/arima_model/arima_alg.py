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

import itertools
from types import SimpleNamespace

import numpy as np
from scipy import optimize
from scipy.signal import lfilter

from ..adf import adfuller
from ..forecasting_utils import lag_matrix, yule_walker, diff_heads, un_diff, \
    ar_trans_params, ar_inv_trans_params, ma_trans_params, ma_inv_trans_params
from ..forecasting_utils import InvalidParameter
from ..linear_models import OLS
from ..forcasting_algorithm import ForecastingAlgorithm

MIN_AR_ORDER, MAX_AR_ORDER = 0, 6
MIN_MA_ORDER, MAX_MA_ORDER = 0, 6
MIN_DIFF_TIMES, MAX_DIFF_TIMES = 0, 3
P_VALUE_THRESHOLD = 0.05


def trans_params(params, k, p, q):
    """
    transform the params to make it easier to be fitted in LBFGS model.
    :param params: type -> ndarray  The ARIMA model params.
    :param k: type->int  if k is 1, ARIMA will do linear-fitting before ARMA fitting.
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    """

    newparams = np.zeros_like(params)
    if k != 0:
        newparams[:k] = params[:k]

    if p != 0:
        newparams[k:k + p] = ar_trans_params(params[k:k + p].copy())

    if q != 0:
        newparams[k + p:] = ma_trans_params(params[k + p:].copy())

    return newparams


def inv_trans_params(params, k, p, q):
    """
    Inverse transform the params to recover params from transform.
    fit p, q for ARIMA model. k and d are known here.
    :param params: type -> ndarray  The ARIMA model params.
    :param k: type->int  if k is 1, ARIMA will do linear-fitting before ARMA fitting.
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    """

    newparams = np.zeros_like(params)
    if k != 0:
        newparams[:k] = params[:k]

    if p != 0:
        newparams[k:k + p] = ar_inv_trans_params(params[k:k + p])

    if q != 0:
        newparams[k + p:] = ma_inv_trans_params(params[k + p:])

    return newparams


def _get_ar_order(y, max_lag):
    """
    In this method, we choose Bayesian Infomation Criterion
    to represent the likelihood of the models with different
    Auto-Correlation order. The model with the best AR order
    will has the least BIC.
    :param y: type -> ndarray
    The input data to help determine the AR order.
    :param max_lag: type -> int
    The method will pick out the best AR order from zero to max_lag
    :return AR order: type -> int
    returns the AR order with best BIC
    """

    lag_mat = lag_matrix(y, max_lag)
    lag_mat = lag_mat[max_lag:len(lag_mat) - max_lag, :]
    lag_x, lag_y = lag_mat[:, 1:], lag_mat[:, :1]

    bics = []
    for lag in range(1, max_lag + 1):
        mod = OLS(lag_x[:, :lag], lag_y)
        mod.fit()
        bics.append((mod.bic, int(mod.dof_model)))  # Polish later: why dof, lag?

    return sorted(bics, key=lambda t: t[0])[0][1]


def _fit_ar_ma_params(y, p, q):
    """
    the start ar/ma coeffs for lbfgs to give a optimal parameters.
    :param y: type->np.array
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    :return params: type->np.array
    """

    maxlag = min(
        int(np.ceil(12.0 * np.power(len(y) / 100.0, 1 / 4.0))),
        len(y) - 1
    )
    ar_order = _get_ar_order(y, maxlag)  # Polish later: bestlag?
    if ar_order + q >= len(y):
        raise InvalidParameter("Start ar order is not valid.")

    lag_mat = lag_matrix(y, ar_order)
    lag_mat = lag_mat[ar_order:len(lag_mat) - ar_order, :]
    lag_x, lag_y = lag_mat[:, 1:], lag_mat[:, :1]
    try:
        coeffs = OLS(lag_x, lag_y).fit()
    except ValueError as e:
        raise InvalidParameter(e)

    resid = lag_y[:, 0] - np.dot(lag_x, coeffs)
    start = ar_order + q - p
    endog_start = max(start, 0)
    resid_start = max(-start, 0)
    lag_endog = lag_matrix(y, p)
    lag_endog = lag_endog[p:len(lag_endog) - p, 1:][endog_start:]
    lag_resid = lag_matrix(resid, q)
    lag_resid = lag_resid[q:len(lag_resid) - q, 1:][resid_start:]
    x_stack = np.column_stack((lag_endog, lag_resid))  # Polish later: check what this is?

    coeffs = OLS(x_stack, y[max(ar_order + q, p):]).fit()
    return coeffs


def _get_predict_mu_coeffs(k, trend_params, ar_params, steps):
    """
    the mu is the constant for predict
    :param k: type->int  if k is 1, ARIMA will do linear-fitting before ARMA fitting.
    :param trend_params: type->np.array
    :param ar_params: type->np.array
    :param steps: type->int
    :return: mu_coeffs: type->np.array
    """
    if k == 1:
        mu_coeffs = trend_params * (1 - ar_params.sum())
        mu_coeffs = np.array([mu_coeffs] * steps)
    else:
        mu_coeffs = np.zeros(steps)

    return mu_coeffs


def _get_resid_out_of_sample(errors, p, q):
    """
    returns resid for sample prediction.
    :param errors: type->np.array
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    :return: resid: type->np.array
    """

    start = 0
    resid = None
    if q:
        resid = np.zeros(q)
        if start:
            resid[:q] = errors[start - q - p:start - p]
        else:
            resid[:q] = errors[-q:]

    return resid


def _get_predict_out_of_sample(steps, trend_params, ar_params, order, once_data):
    """
    returns endog, mu of appropriate length for sample prediction.
    :param steps: type->int
    :param trend_params: type->np.array
    :param ar_params: type->np.array
    :param order: type->tuple
    :param once_data: type->np.array
    :return: endog: type->np.array, mu_coeffs: type->np.array
    """

    k = order.trend
    q = order.ar
    y = once_data.y

    start = 0
    mu_coeffs = _get_predict_mu_coeffs(k, trend_params, ar_params, steps)
    endog = np.zeros(q + steps - 1)

    if q and start:
        endog[:q] = y[start - q:start]
    elif q:
        endog[:q] = y[-q:]

    return endog, mu_coeffs


def _arma_predict_out_of_sample(steps, params, resid, order, once_data):
    """
    predict steps data according to sample.
    :param params: type->np.array
    :param steps: type->int
    :param resid: type->np.array
    :param order: type->tuple
    :param once_data: type->np.array
    :return: forecast: type->np.array
    """

    k = order.trend
    p = order.ar
    q = order.ma

    trend_params = params[:k]
    ar_params = params[k:k + p][::-1]
    ma_params = params[k + p:][::-1]

    resid = _get_resid_out_of_sample(resid, p, q)  # Polish later？
    endog, mu_coeffs = _get_predict_out_of_sample(steps, trend_params, ar_params, order, once_data)

    forecast = np.zeros(steps)
    if steps == 1:
        if q:
            return mu_coeffs[0] + np.dot(ar_params, endog[:p]) + np.dot(ma_params, resid[:q])

        return mu_coeffs[0] + np.dot(ar_params, endog[:p])

    i = 0 if q else -1
    for i in range(min(q, steps - 1)):
        fcast = mu_coeffs[i] + np.dot(ar_params, endog[i:i + p]) + np.dot(ma_params[:q - i], resid[i:i + q])
        forecast[i] = fcast
        endog[i + p] = fcast

    for i in range(i + 1, steps - 1):
        fcast = mu_coeffs[i] + np.dot(ar_params, endog[i:i + p])
        forecast[i] = fcast
        endog[i + p] = fcast

    forecast[steps - 1] = mu_coeffs[steps - 1] + np.dot(ar_params, endog[steps - 1:])
    return forecast


class ARIMA(ForecastingAlgorithm):
    """
    ARIMA is a method which forecast series‘s future according to its own history
    ARIMA = AR(Auto-Regressive) + I(Integrated) + MA(Moving Average)
    """

    def __init__(self, is_transparams=False, linear_fitting=True):
        self.is_transparams = is_transparams
        self.linear_fitting = linear_fitting
        self.original_data = None
        self.order = None
        self.once_data = None
        self.nobs = None
        self.params = None

    def fit(self, sequence):
        self.original_data = np.array(sequence.values).astype('float32')
        # To determine d by Augmented-Dickey-Fuller method.
        n_diff = MIN_DIFF_TIMES
        for n_diff in range(MIN_DIFF_TIMES, MAX_DIFF_TIMES + 1):
            diff_data = np.diff(self.original_data, n=n_diff)
            adf_res = adfuller(diff_data, max_lag=None)
            if adf_res[1] < P_VALUE_THRESHOLD and adf_res[0] < adf_res[4]['5%']:
                d = n_diff
                break
        else:
            d = n_diff

        k = int(self.linear_fitting)
        orders = []
        p_q_pairs = itertools.product(
            range(MIN_AR_ORDER, MAX_AR_ORDER + 1, 2),
            range(MIN_MA_ORDER, MAX_MA_ORDER + 1, 2)
        )
        for p, q in p_q_pairs:  # Look for the optimal parameters (p, q).
            if p == 0 and q == 0:
                continue

            try:
                self.fit_once(k, p, d, q)
                if not np.isnan(self.bic):
                    orders.append((self.bic, p, q))
            except InvalidParameter:
                continue

        _, p0, q0 = sorted(orders)[0]
        for p, q in [(p0-1, q0), (p0, q0-1), (p0+1, q0), (p0, q0+1)]:
            if p < 0 or q < 0:
                continue

            try:
                self.fit_once(k, p, d, q)
                if not np.isnan(self.bic):
                    orders.append((self.bic, p, q))
            except InvalidParameter:
                continue

        for _, p, q in sorted(orders):
            try:
                self.fit_once(k, p, d, q)
                break
            except InvalidParameter:
                continue
        else:
            raise AttributeError('Not any (p, d, q) combination is available.')

    def fit_once(self, k, p, d, q):
        """
        fit p, q for ARIMA model. k and d are known here.
        :param k: type->int  if k is 1, ARIMA will do linear-fitting before ARMA fitting.
        :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                             how many historical data the AR procedure uses.
        :param d: type->int  Integration times which indicate how many times to diff
                             the data to make it stationary.
        :param q: type->int  Moving Average order of the ARIMA model which indicates
                             how many historical resid the MA procedure uses.
        """

        def loglike(params):
            return -self.loglike_css(params) / nobs

        y = np.diff(self.original_data, n=d)
        nobs = len(y)
        x = np.ones((nobs, 1))
        self.once_data = SimpleNamespace(x=x, y=y)
        self.order = SimpleNamespace(trend=k, ar=p, diff=d, ma=q)
        self.nobs = len(y) - p

        start_params = self._fit_start_params()

        lbfgs_attributes = {
            'disp': -1,
            'm': 12,
            'pgtol': 1e-08,
            'factr': 100.0,
            'approx_grad': True,
            'maxiter': 500
        }
        res = optimize.fmin_l_bfgs_b(loglike, start_params, **lbfgs_attributes)
        self.params = res[0]
        if self.is_transparams:
            self.params = inv_trans_params(self.params, k, p, q)

    def _fit_start_params(self):
        """
        compute start coeffs of ar and ma for optimize.
        :return start_params: type->np.array
        """

        k = self.order.trend
        p = self.order.ar
        q = self.order.ma
        x = self.once_data.x
        y = self.once_data.y
        start_params = np.zeros(k + p + q)
        if k != 0:
            ols_params = OLS(x, y).fit()
            start_params[:k] = ols_params
            y = y - np.dot(x, ols_params).squeeze()

        if q != 0 and p != 0:  # Polish later: if else from where?
            start_params[k:] = _fit_ar_ma_params(y, p, q)
        elif q != 0 and p == 0:
            ar_params = yule_walker(y, order=q)
            ar_coeffs = np.r_[[1], -ar_params.squeeze()]
            impulse = np.r_[[1], np.zeros(q)]
            ma_params = lfilter([1], ar_coeffs, impulse)[1:]  # Polish later: ar empty or ma empty?
            start_params[k + p:] = ma_params
        elif q == 0 and p != 0:
            ar_params = yule_walker(y, order=p)
            start_params[k:k + p] = ar_params

        if p and not np.all(np.abs(np.roots(np.r_[1, -start_params[k:k + p]])) < 1):
            raise InvalidParameter("The ar start coeffs %s is invalid." % p)

        if q and not np.all(np.abs(np.roots(np.r_[1, start_params[k + p:]])) < 1):
            raise InvalidParameter("The ma start coeffs %s is invalid." % q)

        if self.is_transparams:
            start_params = trans_params(start_params, k, p, q)

        return start_params

    def forecast(self, steps):
        """
        return the forecast data form history data with ar coeffs,
        ma coeffs and diff order.
        :param steps: type->int
        :return forecast: type->np.array
        """

        forecast = _arma_predict_out_of_sample(steps, self.params, self.resid,
                                               self.order, self.once_data)

        if self.order.diff:
            heads = diff_heads(self.original_data[-self.order.diff:], self.order.diff)
            forecast = un_diff(forecast, heads)[self.order.diff:]
        else:
            forecast += self.original_data[-1] - forecast[0]

        return forecast

    def loglike_css(self, params):
        """
        return the log-likelihood function to compute BIC.
        :param params: type->np.array
        :return llf: type->float
        """

        resid = self.get_resid(params)
        sigma2 = np.sum(resid ** 2) / float(self.nobs)
        llf = -self.nobs * (np.log(2 * np.pi * sigma2) + 1) / 2.0
        return llf

    @property
    def llf(self):
        """
        the llf for residuals estimated is used to compute BIC
        """

        return self.loglike_css(self.params)

    @property
    def bic(self):
        """
        Bayesian Infomation Criterion
        the BIC is for measuring the criterion of the model.
        """

        dof_model = self.order.trend + self.order.ar + self.order.ma
        return -2 * self.llf + np.log(self.nobs) * dof_model

    def get_resid(self, params=None):
        """
        return the resid related to moving average
        :param params: type->np.array
        :return resid: type->np.array
        """

        if params is None:
            params = self.params

        params = np.asarray(params)
        k = self.order.trend
        p = self.order.ar
        q = self.order.ma
        y = self.once_data.y.copy()

        if k > 0:
            y = y - np.dot(self.once_data.x, params[:k])

        ar_params = params[k:k + p]
        ma_params = params[k + p:]
        ar_coeffs = np.r_[1, -ar_params]
        ma_coeffs = np.r_[1, ma_params]

        zi = np.zeros((max(p, q)))  # Polish later: what's this, discard?
        for i in range(p):
            zi[i] = sum(-ar_coeffs[:i + 1][::-1] * y[:i + 1])

        err = lfilter(ar_coeffs, ma_coeffs, y, zi=zi)  # Polish later: ?
        resid = err[0][p:]  # Polish later: why kar, whole?
        return resid

    @property
    def resid(self):
        return self.get_resid()
