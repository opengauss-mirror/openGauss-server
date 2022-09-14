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
from scipy.linalg import toeplitz


class InvalidParameter(Exception):
    pass


def lag_matrix(x, nlags):
    """\
    input a 1D array x and nlags
    returns a 2D array 'lag_matrix' ((len(x) + nlags) * (nlags + 1))
    which shows observation in columns with different lags.
    ┌   ┐         ┌            ┐
    │ 1 │         │ 1  0  0  0 │
    │ 2 │  lag=3  │ 2  1  0  0 │
    │ 3 │  ─────→ │ 3  2  1  0 │
    │ 4 │         │ 4  3  2  1 │
    └   ┘         │ 0  4  3  2 │
                  │ 0  0  4  3 │
                  │ 0  0  0  4 │
                  └            ┘
    """

    n = x.shape[0]
    if x.ndim > 2 or (x.ndim == 2 and x.shape[1] > 1):
        raise ValueError('The input array x must be a vector.')

    if x.ndim == 2 and x.shape[1] == 1:
        x = x.flatten()

    if nlags >= n:
        raise ValueError("nlags should be shorter than the size of x")

    res = np.zeros((n + nlags, nlags + 1))
    for i in range(0, nlags + 1):
        res[i:i+n, i] = x

    return res


def ar_trans_params(params):
    """
    transforms ar params to induce stationarity/invertability.
    :param params: type->np.array
    :return newparams: type->np.array
    """

    newparams = np.tanh(params / 2)
    tmp = np.tanh(params / 2)
    for i in range(1, len(params)):
        ar_param = newparams[i]
        for j in range(i):
            tmp[j] -= ar_param * newparams[i - j - 1]

        newparams[:i] = tmp[:i]

    return newparams


def ar_inv_trans_params(params):
    """
    return the inverse of the ar params.
    :param params: type->np.array
    :return invarcoefs: type->np.array
    """

    params = params.copy()
    tmp = params.copy()
    for i in range(len(params) - 1, 0, -1):
        ar_param = params[i]
        for j in range(i):
            tmp[j] = (params[j] + ar_param * params[i - j - 1]) / (1 - ar_param ** 2)

        params[:i] = tmp[:i]

    inv_ar_params = 2 * np.arctanh(params)
    return inv_ar_params


def ma_trans_params(params):
    """
    transforms ma params to induce stationarity/invertability.
    :param params: type->np.array
    :return newparams: type->np.array
    """

    newparams = ((1 - np.exp(-params)) / (1 + np.exp(-params))).copy()
    tmp = ((1 - np.exp(-params)) / (1 + np.exp(-params))).copy()

    for i in range(1, len(params)):
        ma_param = newparams[i]
        for j in range(i):
            tmp[j] += ma_param * newparams[i - j - 1]

        newparams[:i] = tmp[:i]

    return newparams


def ma_inv_trans_params(params):
    """
    return the inverse of the ma params.
    :return invmacoefs: type->np.array
    """

    tmp = params.copy()
    for i in range(len(params) - 1, 0, -1):
        ma_param = params[i]
        for j in range(i):
            tmp[j] = (params[j] - ma_param * params[i - j - 1]) / (1 - ma_param ** 2)

        params[:i] = tmp[:i]

    inv_ma_params = -np.log((1 - params) / (1 + params))
    return inv_ma_params


def yule_walker(x, order=1):
    """
    estimate ar parameters.
    :param x type->np.array
    :param order: type->tuple
    :return rho: type->np.array,
    """

    x = np.array(x, dtype=np.float64)
    x -= x.mean()
    num = x.shape[0]

    if x.ndim > 1 and x.shape[1] != 1:
        raise InvalidParameter("expecting a vector to estimate ar parameters")

    r = np.zeros(order + 1, np.float64)
    r[0] = (x ** 2).sum() / num
    for k in range(1, order + 1):
        r[k] = (x[0:-k] * x[k:]).sum() / (num - k)

    R = toeplitz(r[:-1])
    try:
        rho = np.linalg.solve(R, r[1:])
        return rho
    except np.linalg.LinAlgError as e:
        raise InvalidParameter(e)


def un_diff(x, heads):
    """
    After taking n-differences of a series, return the original series
    :param x: type->np.array  diff_data
    :param heads: type->np.array  diff_heads
    :return: original_data: type->np.array
    """

    heads = list(heads)[:]  # copy
    if len(heads) > 1:
        x0 = heads.pop(-1)
        return un_diff(np.cumsum(np.r_[x0, x]), heads)
    x0 = heads[0]
    return np.cumsum(np.r_[x0, x])


def diff_heads(x, d):
    """
    Returns the first element in order to recover the diff data.
    :param x: type->np.array  original data
    :param d: type->int  diff times
    :return: diff heads: type->np.array
    """

    x = x[:d]
    return np.asarray([np.diff(x, n=i)[0] for i in range(d)])
