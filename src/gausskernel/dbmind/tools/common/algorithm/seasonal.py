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
from scipy import signal
from .stat_utils import trim_head_and_tail_nan
import warnings

warnings.filterwarnings("ignore")


def acf(x_raw: np.array, nlags=None):
    x = np.array(x_raw)
    n = x.shape[0]
    if nlags is None:
        nlags = min(int(10 * np.log10(n)), n - 1)

    x_diff = x - x.mean()
    avf = np.correlate(x_diff, x_diff, "full")[n - 1:] / n
    res = avf[: nlags + 1] / avf[0]
    return res


def _padding_nans(x_raw, trim_head=None, trim_tail=None):
    """padding nans in head or tail of x_raw"""
    result = None
    if trim_head is None and trim_tail is None:
        result = x_raw
    elif trim_tail is None:
        result = np.r_[[np.nan] * trim_head, x_raw]
    elif trim_head is None:
        result = np.r_[x_raw, [np.nan] * trim_tail]
    elif trim_head and trim_tail:
        result = np.r_[[np.nan] * trim_head, x_raw, [np.nan] * trim_tail]
    return result


def _get_trend(x_raw, filter_):
    """"use the filter to extract trend component"""
    length = len(filter_)
    trim_tail = (length - 1) // 2 or None
    trim_head = length - 1 - trim_tail or None
    result = signal.convolve(x_raw, filter_, mode='valid')
    result = _padding_nans(result, trim_head, trim_tail)
    return result


def is_seasonal_series(s_values, high_ac_threshold: float = 0.7, min_seasonal_freq=3):
    """Judge whether the series is seasonal by using the acf alg"""
    s_ac = acf(s_values, nlags=len(s_values))
    diff_ac = np.diff(s_ac)
    high_ac_peak_pos = 1 + np.argwhere(
        (diff_ac[:-1] > 0) & (diff_ac[1:] < 0) & (s_ac[1: -1] > high_ac_threshold)
    ).flatten()

    for i in high_ac_peak_pos:
        if i > min_seasonal_freq:
            return True, high_ac_peak_pos[np.argmax(s_ac[high_ac_peak_pos])]

    return False, None


def get_seasonal_period(s_values, high_ac_threshold: float = 0.5, min_seasonal_freq=3):
    """"return seasonal period"""
    return is_seasonal_series(s_values, high_ac_threshold, min_seasonal_freq)[1]


def _get_filter(period):
    """the filter to extract trend component"""
    if period % 2 == 0:
        filter_ = np.array([.5] + [1] * (period - 1) + [.5]) / period
    else:
        filter_ = np.repeat(1. / period, period)
    return filter_


def _get_seasonal(x_raw, detrended, period):
    """"return the seasonal component from x_raw, detrended and period"""
    nobs = len(x_raw)
    period_averages = np.array([np.nanmean(detrended[i::period]) for i in range(period)])
    period_averages -= np.mean(period_averages, axis=0)
    seasonal = np.tile(period_averages.T, nobs // period + 1).T[:nobs]

    return seasonal


def seasonal_decompose(x_raw, period=None):
    """decompose a series into three components: seasonal, trend, residual"""

    if np.ndim(x_raw) > 1:
        raise ValueError("The input data must be 1-D array.")

    if period is None:
        raise ValueError("You must specify a period.")

    if not np.all(np.isfinite(x_raw)):
        raise ValueError("The input data has infinite value or nan.")

    if x_raw.shape[0] < 2 * period:
        raise ValueError(f"The input data should be longer than two periods:{2 * period} at least")

    x_raw = trim_head_and_tail_nan(x_raw)
    trend = _get_trend(x_raw, _get_filter(period))
    trend = trim_head_and_tail_nan(trend)
    detrended = x_raw - trend

    seasonal = _get_seasonal(x_raw, detrended, period)
    seasonal = trim_head_and_tail_nan(seasonal)
    resid = detrended - seasonal
    return seasonal, trend, resid
