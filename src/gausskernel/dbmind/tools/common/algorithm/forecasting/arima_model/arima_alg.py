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
import logging
import time
from numpy import dot, log, zeros, pi
from scipy import optimize
from scipy import signal
from scipy.signal import lfilter
from .arima_common import lagmat, OLS


def _ar_transparams(params):
    """
    transforms ar params to induce stationarity/invertability.
    :param params: type->np.array
    :return newparams: type->np.array
    """
    newparams = np.tanh(params / 2)
    tmp = np.tanh(params / 2)
    for j in range(1, len(params)):
        ar_param = newparams[j]
        for kiter in range(j):
            tmp[kiter] -= ar_param * newparams[j - kiter - 1]
        newparams[:j] = tmp[:j]
    return newparams


def _ar_invtransparams(params):
    """
    return the inverse of the ar params.
    :param params: type->np.array
    :return invarcoefs: type->np.array
    """
    params = params.copy()
    tmp = params.copy()
    for j in range(len(params) - 1, 0, -1):
        ar_param = params[j]
        for kiter in range(j):
            tmp[kiter] = (params[kiter] + ar_param * params[j - kiter - 1]) / \
                         (1 - ar_param ** 2)
        params[:j] = tmp[:j]
    invarcoefs = 2 * np.arctanh(params)
    return invarcoefs


def _ma_transparams(params):
    """
    transforms ma params to induce stationarity/invertability.
    :param params: type->np.array
    :return newparams: type->np.array
    """
    newparams = ((1 - np.exp(-params)) / (1 + np.exp(-params))).copy()
    tmp = ((1 - np.exp(-params)) / (1 + np.exp(-params))).copy()

    for j in range(1, len(params)):
        ma_param = newparams[j]
        for kiter in range(j):
            tmp[kiter] += ma_param * newparams[j - kiter - 1]
        newparams[:j] = tmp[:j]
    return newparams


def _ma_invtransparams(macoefs):
    """
    return the inverse of the ma params.
    :param params: type->np.array
    :return invmacoefs: type->np.array
    """
    tmp = macoefs.copy()
    for j in range(len(macoefs) - 1, 0, -1):
        ma_param = macoefs[j]
        for kiter in range(j):
            tmp[kiter] = (macoefs[kiter] - ma_param *
                          macoefs[j - kiter - 1]) / (1 - ma_param ** 2)
        macoefs[:j] = tmp[:j]
    invmacoefs = -np.log((1 - macoefs) / (1 + macoefs))
    return invmacoefs


class DummyArray:
    """ support __array_interface__ and base"""

    def __init__(self, interface, base=None):
        self.__array_interface__ = interface
        self.base = base

    def wr_dummy(self):
        """write dummy"""

    def rd_dummy(self):
        """read dummy"""


def _maybe_view_as_subclass(original_array, new_array):
    """
    return new_array for view_as_subclass.
    :param original_array: type->np.array
    :return new_array: type->np.array
    """
    if type(original_array) is not type(new_array):
        new_array = new_array.view(type=type(original_array))
        if new_array.__array_finalize__:
            new_array.__array_finalize__(original_array)
    return new_array


def _as_strided(x_raw, shape=None, strides=None):
    """
    create a view into the array with the given shape and strides.
    :param x_raw: type->np.array
    :param shape: type->tuple
    :param strides: type->tuple
    :return view: type->np.array
    """
    subok = False
    writeable = True
    x_raw = np.array(x_raw, copy=False, subok=subok)
    interface = dict(x_raw.__array_interface__)
    if shape is not None:
        interface['shape'] = tuple(shape)
    if strides is not None:
        interface['strides'] = tuple(strides)

    array = np.asarray(DummyArray(interface, base=x_raw))
    array.dtype = x_raw.dtype

    view = _maybe_view_as_subclass(x_raw, array)

    if view.flags.writeable and not writeable:
        view.flags.writeable = False

    return view


def _toeplitz(c_raw, r_raw=None):
    """
    construct a toeplitz matrix.
    :param c_raw: type->np.array
    :param r_raw: type->np.array
    :return view: type->np.array
    """
    c_raw = np.asarray(c_raw).ravel()
    if r_raw is None:
        r_raw = c_raw.conjugate()
    else:
        r_raw = np.asarray(r_raw).ravel()

    vals = np.concatenate((c_raw[::-1], r_raw[1:]))
    out_shp = len(c_raw), len(r_raw)
    num = vals.strides[0]
    return _as_strided(vals[len(c_raw) - 1:], shape=out_shp, strides=(-num, num)).copy()


def yule_walker(x_raw, order=1):
    """
    estimate ar parameters from a sequence.
    :param x_raw type->np.array
    :param order: type->tuple
    :return rho: type->np.array,
    """
    method = "adjusted"
    df_raw = None
    demean = True
    x_raw = np.array(x_raw, dtype=np.float64)
    if demean:
        x_raw -= x_raw.mean()
    num = df_raw or x_raw.shape[0]

    adj_needed = method == "adjusted"

    if x_raw.ndim > 1 and x_raw.shape[1] != 1:
        raise ValueError("expecting a vector to estimate ar parameters")
    r_raw = np.zeros(order + 1, np.float64)
    r_raw[0] = (x_raw ** 2).sum() / num
    for k in range(1, order + 1):
        r_raw[k] = (x_raw[0:-k] * x_raw[k:]).sum() / (num - k * adj_needed)
    r_tope = _toeplitz(r_raw[:-1])
    rho = np.linalg.solve(r_tope, r_raw[1:])

    return rho


def _arma_impulse_response(new_ar_coeffs, new_ma_coeffs, leads=100):
    """
    compute the impulse response function (MA representation) for ARMA process
    :param new_ar_coeffs: type->np.array
    :param new_ma_coeffs: type->np.array
    :param leads: type->int
    :return: impulse response: type->np.array
    """
    impulse = np.zeros(leads)
    impulse[0] = 1.
    return signal.lfilter(new_ma_coeffs, new_ar_coeffs, impulse)


def _unpack_params(params, order, k_trend, reverse=False):
    """
    unpack trend, exparams, arparams, maparams from params
    :param params: type->np.array
    :param order: type->tuple
    :param k_trend: type->int
    :param reverse: type->bool
    :return: trend: type->np.array, exparams: type->np.array,
            arparams: type->np.array, maparams: type->np.array
    """
    k_ar, _ = order
    k = k_trend
    maparams = params[k + k_ar:]
    arparams = params[k:k + k_ar]
    trend = params[:k_trend]
    exparams = params[k_trend:k]
    if reverse:
        return trend, exparams, arparams[::-1], maparams[::-1]
    return trend, exparams, arparams, maparams


def _get_predict_mu_coeffs(k_trend, trendparam, arparams, steps):
    """
    the mu is the constant for predict
    :param k_trend: type->int
    :param trendparam: type->np.array
    :param arparams: type->np.array
    :param steps: type->int
    :return: mu_coeffs: type->np.array
    """
    if k_trend == 1:
        mu_coeffs = trendparam * (1 - arparams.sum())
        mu_coeffs = np.array([mu_coeffs] * steps)
    else:
        mu_coeffs = np.zeros(steps)

    return mu_coeffs


def _get_resid_out_of_sample(errors, order):
    """
    returns resid for sample prediction.
    :param errors: type->np.array
    :param order: type->tuple
    :return: resid: type->np.array
    """
    k_ma = order.k_ma
    k_ar = order.k_ar
    start = 0
    resid = None
    if k_ma:
        resid = np.zeros(k_ma)
        if start:
            resid[:k_ma] = errors[start - k_ma - k_ar:start - k_ar]
        else:
            resid[:k_ma] = errors[-k_ma:]
    return resid


def _get_predict_out_of_sample(raw_data, order, trendparam, arparams, steps):
    """
    returns endog, mu of appropriate length for sample prediction.
    :param raw_data: type->np.array
    :param order: type->tuple
    :param trendparam: type->np.array
    :param arparams: type->np.array
    :param steps: type->int
    :return: endog: type->np.array, mu_coeffs: type->np.array
    """
    k_ar = order.k_ar
    start = 0

    y_raw = raw_data.y
    mu_coeffs = _get_predict_mu_coeffs(raw_data.k_trend, trendparam, arparams, steps)
    endog = np.zeros(k_ar + steps - 1)

    if k_ar and start:
        endog[:k_ar] = y_raw[start - k_ar:start]
    elif k_ar:
        endog[:k_ar] = y_raw[-k_ar:]

    return endog, mu_coeffs


def _arma_predict_out_of_sample(params, steps, errors, order, raw_data):
    """
    predict steps data according to sample.
    :param params: type->np.array
    :param steps: type->int
    :param errors: type->np.array
    :param order: type->tuple
    :param raw_data: type->np.array
    :return: forecast: type->np.array
    """
    (trendparam, _,
     arparams, maparams) = _unpack_params(params, (order.k_ar, order.k_ma),
                                          raw_data.k_trend, reverse=True)
    resid = _get_resid_out_of_sample(errors, order)
    endog, mu_coeffs = _get_predict_out_of_sample(raw_data,
                                                  order,
                                                  trendparam,
                                                  arparams,
                                                  steps)

    forecast = np.zeros(steps)
    if steps == 1:
        if order.k_ma:
            return mu_coeffs[0] + np.dot(arparams, endog[:order.k_ar]) \
                   + np.dot(maparams, resid[:order.k_ma])

        return mu_coeffs[0] + np.dot(arparams, endog[:order.k_ar])

    i = 0 if order.k_ma else -1
    for i in range(min(order.k_ma, steps - 1)):
        fcast = (mu_coeffs[i] + np.dot(arparams, endog[i:i + order.k_ar]) +
                 np.dot(maparams[:order.k_ma - i], resid[i:i + order.k_ma]))
        forecast[i] = fcast
        endog[i + order.k_ar] = fcast

    for i in range(i + 1, steps - 1):
        fcast = mu_coeffs[i] + np.dot(arparams, endog[i:i + order.k_ar])
        forecast[i] = fcast
        endog[i + order.k_ar] = fcast

    forecast[steps - 1] = mu_coeffs[steps - 1] + np.dot(arparams, endog[steps - 1:])
    return forecast


def unintegrate_levels(x_raw, k_diff):
    """
    returns the successive differences needed to unintegrate the series.
    :param x_raw: type->np.array
    :param k_diff: type->int
    :return: unintegrated series: type->np.array
    """
    x_raw = x_raw[:k_diff]
    return np.asarray([np.diff(x_raw, k_diff - i)[0] for i in range(k_diff, 0, -1)])


def unintegrate(x_raw, levels):
    """
    after taking n-differences of a series, return the original series
    :param x_raw: type->np.array
    :param levels: type->np.array
    :return: original series: ype->np.array
    """
    x0_raw = []
    levels = list(levels)[:]
    if len(levels) > 1:
        x0_raw = levels.pop(-1)
        return unintegrate(np.cumsum(np.r_[x0_raw, x_raw]), levels)
    if len(levels) != 0:
        x0_raw = levels[0]
    return np.cumsum(np.r_[x0_raw, x_raw])


def _get_ar_order(y_raw, maxlag):
    """
    the ar order computed by bic is used for OLS
    :param y_raw: type->np.array
    :param maxlag: type->int
    :return ar_order: type->int
    """
    nexog = 0
    x_mat, y_mat = lagmat(y_raw, maxlag, original="sep")
    _y_mat = y_mat[maxlag:]
    _x_mat = x_mat[maxlag:]
    base_col = x_mat.shape[1] - nexog - maxlag
    sel = np.ones(x_mat.shape[1], dtype=bool)
    sel[base_col: base_col + maxlag] = False

    min_bic = np.inf
    ar_order = 0
    for i in range(maxlag + 1):
        sel[base_col: base_col + i] = True
        if not np.any(sel):
            continue
        mod = OLS(_y_mat, _x_mat[:, sel])
        mod.fit()
        sigma2 = 1.0 / mod.nobs * np.sum(mod.wresid ** 2, axis=0)
        bic = np.log(sigma2) + (1 + mod.df_model) * np.log(mod.nobs) / mod.nobs
        if bic < min_bic:
            min_bic = bic
            ar_order = mod.df_model
    ar_order = int(ar_order)
    return ar_order


def _get_lag_data_and_resid(y_raw, p_tmp, arcoefs_tmp, order):
    """
    the lag_endog and lag_resid are used to fit coeffs.
    :param y_raw: type->np.array
    :param p_tmp: type->np.array
    :param arcoefs_tmp: type->np.array
    :param arcoefs_tmp: type->np.array
    :param order: type->tuple
    :return lag_endog: type->np.array, lag_resid: type->np.array
    """
    k_ar, k_ma = order
    resid = y_raw[p_tmp:] - np.dot(lagmat(y_raw, p_tmp, trim='both'), arcoefs_tmp)
    if k_ar < p_tmp + k_ma:
        endog_start = p_tmp + k_ma - k_ar
        resid_start = 0
    else:
        endog_start = 0
        resid_start = k_ar - p_tmp - k_ma
    lag_endog = lagmat(y_raw, k_ar, 'both')[endog_start:]
    lag_resid = lagmat(resid, k_ma, 'both')[resid_start:]

    return lag_endog, lag_resid


def _compute_start_ar_ma_coeffs(k_ar, k_ma, y_raw):
    """
    the start ar/ma coeffs for lbfgs to give a optimal parameters.
    :param k_ar: type->np.array
    :param k_ma: type->np.array
    :param y_raw: type->np.array
    :return coeffs: type->np.array
    """
    nobs = len(y_raw)
    maxlag = int(round(12 * (nobs / 100.) ** (1 / 4.)))
    if maxlag >= nobs:
        maxlag = nobs - 1

    ar_order = _get_ar_order(y_raw, maxlag)
    _x_mat, _y_mat = lagmat(y_raw, ar_order, original="sep")
    _y_mat = _y_mat[ar_order:]
    _x_mat = _x_mat[ar_order:]
    ols_mod = OLS(_y_mat, _x_mat)
    ols_res = ols_mod.fit()
    arcoefs_tmp = ols_res

    if ar_order + k_ma >= len(y_raw):
        raise ValueError("start ar order is not valid")

    lag_endog, lag_resid = _get_lag_data_and_resid(y_raw,
                                                   ar_order,
                                                   arcoefs_tmp,
                                                   (k_ar, k_ma))
    x_stack = np.column_stack((lag_endog, lag_resid))
    coeffs = OLS(y_raw[max(ar_order + k_ma, k_ar):], x_stack).fit()

    return coeffs


def _get_errors(params, raw_data, order):
    """
    return the errors model predict data and raw data for forecast.
    :param params: type->np.array
    :param raw_data: type->np.array
    :param order: type->tuple
    :return errors: type->np.array
    """
    params = np.asarray(params)
    y_raw = raw_data.y.copy()
    k_ar = order.k_ar
    k_ma = order.k_ma

    if raw_data.k_trend > 0:
        y_raw -= dot(raw_data.x, params[:raw_data.k_trend])

    (_, _, arparams, maparams) = _unpack_params(params, (k_ar, k_ma),
                                                raw_data.k_trend, reverse=False)
    ar_c = np.r_[1, -arparams]
    ma_c = np.r_[1, maparams]
    zi_raw = zeros((max(k_ar, k_ma)))
    for i in range(k_ar):
        zi_raw[i] = sum(-ar_c[:i + 1][::-1] * y_raw[:i + 1])
    err = lfilter(ar_c, ma_c, y_raw, zi=zi_raw)
    errors = err[0][k_ar:]
    return errors


class ARIMA:
    """ARIMA model can forecast series according to history series"""

    def __init__(self, y_raw, order):
        """
        :param y_raw: type->np.array
        :param order: type->tuple
        """
        k_ar, k_diff, k_ma = order
        self.order = SimpleNamespace(k_ar=k_ar, k_diff=k_diff, k_ma=k_ma)
        y_raw = np.asarray(y_raw) if isinstance(y_raw, (list, tuple)) else y_raw
        y_fit = np.diff(y_raw, n=k_diff)
        x_fit = np.ones((len(y_fit), 1))
        self.raw_data = SimpleNamespace(x=x_fit, y=y_fit, raw_y=y_raw, k_trend=1)
        self.nobs = len(y_fit) - k_ar
        self.is_transparams = True
        self.resid = None
        self.params = None

    def _fit_start_coeffs(self, order):
        """
        compute start coeffs of ar and ma for optimize.
        :param order: type->tuple
        :return start_params: type->np.array
        """
        k_ar, k_ma, k_trend = order
        start_params = zeros((k_ar + k_ma + k_trend))

        y_raw = np.array(self.raw_data.y, np.float64)
        x_raw = self.raw_data.x
        if k_trend != 0:
            ols_params = OLS(y_raw, x_raw).fit()
            start_params[:k_trend] = ols_params
            y_raw -= np.dot(x_raw, ols_params).squeeze()
        if k_ma != 0:
            if k_ar != 0:
                start_params[k_trend:k_trend + k_ar + k_ma] = \
                    _compute_start_ar_ma_coeffs(k_ar, k_ma, y_raw)
            else:
                ar_coeffs = yule_walker(y_raw, order=k_ma)
                new_ar_coeffs = np.r_[[1], -ar_coeffs.squeeze()]
                start_params[k_trend + k_ar:k_trend + k_ar + k_ma] = \
                    _arma_impulse_response(new_ar_coeffs, [1], leads=k_ma + 1)[1:]
        if k_ma == 0 and k_ar != 0:
            arcoefs = yule_walker(y_raw, order=k_ar)
            start_params[k_trend:k_trend + k_ar] = arcoefs

        if k_ar and not np.all(np.abs(np.roots(np.r_[1, -start_params[k_trend:k_trend + k_ar]]
                                               )) < 1):
            raise ValueError("the ar start coeffs is invalid")
        if k_ma and not np.all(np.abs(np.roots(np.r_[1, start_params[k_trend + k_ar:]]
                                               )) < 1):
            raise ValueError("the ma start coeffs is invalid")

        return self._invtransparams(start_params)

    def loglike_css(self, params):
        """
        return the llf to compute BIC.
        :param params: type->np.array
        :return llf: type->float
        """
        nobs = self.nobs
        if self.is_transparams:
            newparams = self._transparams(params)
        else:
            newparams = params
        errors = _get_errors(newparams, self.raw_data, self.order)

        ssr = np.dot(errors, errors)
        sigma2 = ssr / nobs
        llf = -nobs / 2. * (log(2 * pi) + log(sigma2)) - ssr / (2 * sigma2)
        return llf

    def _transparams(self, params):
        """
        return the trans of coeffs.
        :param params: type->np.array
        :return newparams: type->np.array
        """
        k_ar, k_ma = self.order.k_ar, self.order.k_ma
        k = self.raw_data.k_trend
        newparams = np.zeros_like(params)

        if k != 0:
            newparams[:k] = params[:k]

        if k_ar != 0:
            newparams[k:k + k_ar] = _ar_transparams(params[k:k + k_ar].copy())

        if k_ma != 0:
            newparams[k + k_ar:] = _ma_transparams(params[k + k_ar:].copy())
        return newparams

    def _invtransparams(self, start_params):
        """
        return the inverse of the coeffs.
        :param start_params: type->np.array
        :return newparams: type->np.array
        """
        k_ar, k_ma = self.order.k_ar, self.order.k_ma
        k = self.raw_data.k_trend
        newparams = start_params.copy()
        arcoefs = newparams[k:k + k_ar]
        macoefs = newparams[k + k_ar:]

        if k_ar != 0:
            newparams[k:k + k_ar] = _ar_invtransparams(arcoefs)

        if k_ma != 0:
            newparams[k + k_ar:k + k_ar + k_ma] = _ma_invtransparams(macoefs)
        return newparams

    @property
    def llf(self):
        """the llf for errors estimated is used to compute BIC"""
        return self.loglike_css(self.params)

    @property
    def bic(self):
        """the BIC is for optimal parameters:(p d q)"""
        nobs = self.nobs
        df_model = self.raw_data.k_trend + self.order.k_ar + self.order.k_ma
        return -2 * self.llf + np.log(nobs) * (df_model + 1)

    def fit(self, sequence=None):
        """
        fit trend_coeffs, ar_coeffs, ma_coeffs for ARIMA model.
        :return None
        """
        k = self.raw_data.k_trend
        nobs = self.raw_data.y.shape[0]
        start_params = self._fit_start_coeffs((self.order.k_ar, self.order.k_ma, k))

        def loglike(params, *args):
            return -self.loglike_css(params) / nobs

        kwargs = {'m': 12, 'pgtol': 1e-08, 'factr': 100.0, 'approx_grad': True, 'maxiter': 500}
        retvals = optimize.fmin_l_bfgs_b(loglike, start_params, disp=-1, **kwargs)
        xopt = retvals[0]
        if self.is_transparams:
            self.params = self._transparams(xopt)
        else:
            self.params = xopt
        self.is_transparams = False

    def forecast(self, steps):
        """
        return the forecast data form history data with ar coeffs,
        ma coeffs and diff order.
        :param steps: type->int
        :return forecast: type->np.array
        """
        ctime = int(time.time())
        logging.debug("[ARIMA:forecast:%s]: steps:%s, order:%s, coeffs:%s" %
                      (ctime, steps, self.order, self.params))
        logging.debug("[ARIMA:forecast:%s]: raw_data:%s" % (ctime, self.raw_data.y))
        self.resid = _get_errors(self.params, self.raw_data, self.order).squeeze()
        forecast = _arma_predict_out_of_sample(self.params, steps, self.resid,
                                               self.order, self.raw_data)

        forecast = unintegrate(
            forecast,
            unintegrate_levels(
                self.raw_data.raw_y[-self.order.k_diff:],
                self.order.k_diff
            )
        )[self.order.k_diff:]
        logging.debug("[ARIMA:forecast:%s]: forecast result: %s" % (ctime, forecast))
        return forecast
