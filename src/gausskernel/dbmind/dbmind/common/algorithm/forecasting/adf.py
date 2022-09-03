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

from .forecasting_utils import lag_matrix
from .linear_models import OLS
from .adf_values import mackinnoncrit, mackinnonp


def autolag(mod, x, y, start_lag, max_lag):
    """
    Try different lags with least-square model to calculate the best lag with least bic.
    """

    results = {}
    for lag in range(start_lag + 1, start_lag + max_lag + 1):
        mod_instance = mod(x[:, :lag], y)
        mod_instance.fit()
        results[lag] = mod_instance.bic

    best_bic, bestlag = min((v, k) for k, v in results.items())
    return best_bic, bestlag


def add_trend(x, trend_order=1, prepend=False):
    """
    Add trends with orders from 0 to trend_order to the input x and return the added one.
    trend_order : {0, 1, 2, 3}
    trend order to include in regression.
    * 0 : no constant, no trend.
    * 1 : constant only (default).
    * 2 : constant and trend.
    * 3 : constant, and linear and quadratic trend.
    """

    if trend_order == 0:
        return x

    x = np.array(x)
    trend_arr = np.vander(np.arange(1, len(x) + 1, dtype=np.float64), trend_order)
    trend_arr = np.fliplr(trend_arr)  # [1, i, i², ... iⁿ] n=trend_order-1

    # If there is at least one column in x which is non-zero-constant,
    # then we don't need to add the constant trend into x any more.
    col_is_const = np.ptp(x, axis=0) == 0
    non_zero_const = col_is_const & (x[0] != 0)
    if np.any(non_zero_const):
        trend_arr = trend_arr[:, 1:]

    x = [trend_arr, x] if prepend else [x, trend_arr]
    return np.column_stack(x)


def adfuller(x, max_lag=None, trend_order=1):
    """
    The Augmented Dickey-Fuller test can be used to test for a unit root in a
    univariate process in the presence of serial correlation.
    trend_order : {0, 1, 2, 3}
    trend order to include in regression.
    * 0 : no constant, no trend.
    * 1 : constant only (default).
    * 2 : constant and trend.
    * 3 : constant, and linear and quadratic trend.
    """

    L = x.shape[0]
    x_diff = np.diff(x)
    if max_lag is None:
        max_lag = int(np.ceil(12.0 * np.power(L / 100.0, 1 / 4.0)))
        max_lag = min(L // 2 - trend_order - 1, max_lag)
        if max_lag < 0:
            raise ValueError("Sample size is too small.")

    elif max_lag > L // 2 - trend_order - 1:
        ValueError("max_lag is too large.")

    # calculate ols with maxlag
    x_diff_all = lag_matrix(x_diff, max_lag)
    x_diff_all = x_diff_all[max_lag:len(x_diff_all) - max_lag, :]
    nobs = x_diff_all.shape[0]
    x_diff_all[:, 0] = x[-nobs - 1: -1]  # replace 0 x_diff with level of x
    x_diff_short = x_diff[-nobs:]

    full_rhs = add_trend(x_diff_all, prepend=True, trend_order=trend_order)
    start_lag = full_rhs.shape[1] - x_diff_all.shape[1]
    best_bic, best_lag = autolag(OLS, full_rhs, x_diff_short, start_lag, max_lag)
    best_lag -= start_lag  # convert to lag not column index

    # calculate ols again with best_lag
    x_diff_all = lag_matrix(x_diff, best_lag)
    x_diff_all = x_diff_all[best_lag:len(x_diff_all) - best_lag, :]
    nobs = x_diff_all.shape[0]
    x_diff_all[:, 0] = x[-nobs - 1: -1]  # replace 0 x_diff with level of x
    x_diff_short = x_diff[-nobs:]

    mod = OLS(add_trend(x_diff_all[:, : best_lag + 1], trend_order=trend_order), x_diff_short)
    mod.fit()
    adf_stat = mod.tvalues[0]
    # Get approx p-value and critical values
    p_value = mackinnonp(adf_stat, trend_order=trend_order)
    crits = mackinnoncrit(trend_order=trend_order)
    crit_values = {"1%": crits[0], "5%": crits[1], "10%": crits[2]}
    return adf_stat, p_value, best_lag, nobs, crit_values, best_bic
