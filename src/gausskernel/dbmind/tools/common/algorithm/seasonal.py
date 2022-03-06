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
from .statistics import trim_head_and_tail_nan
import warnings
warnings.filterwarnings("ignore")

def acf(x_raw: np, nlags=None):
    """the acf can compute correlation from x[t] and x[t -k]"""
    x_raw = np.array(x_raw)
    x_diff = x_raw - x_raw.mean()
    n_x = len(x_raw)
    d_param = n_x * np.ones(2 * n_x - 1)
    acov = np.correlate(x_diff, x_diff, "full")[n_x - 1:] / d_param[n_x - 1:]
    return acov[: nlags + 1] / acov[0]


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


def _get_trend(x_raw, filt):
    """"use filt to extract trend component"""
    trim_head = int(np.ceil(len(filt) / 2.) - 1) or None
    trim_tail = int(np.ceil(len(filt) / 2.) - len(filt) % 2) or None
    result = signal.convolve(x_raw, filt, mode='valid')
    result = _padding_nans(result, trim_head, trim_tail)

    return result


def is_seasonal_series(s_values, high_ac_threshold: float = 0.7, min_seasonal_freq=3):
    """judge series whether is seasonal with acf alg"""
    result = False
    period = None
    s_ac = acf(s_values, nlags=len(s_values))
    diff_ac = np.diff(s_ac)
    high_ac_peak_pos = (1 + np.argwhere((diff_ac[:-1] > 0) & (diff_ac[1:] < 0)
                                        & (s_ac[1: -1] > high_ac_threshold)).flatten())

    for i in high_ac_peak_pos:
        if i > min_seasonal_freq:
            period = high_ac_peak_pos[np.argmax(s_ac[high_ac_peak_pos])]
            result = True
            break
    return result, period


def get_seasonal_period(s_values, high_ac_threshold: float = 0.5):
    """"return seasonal period"""
    result = is_seasonal_series(s_values, high_ac_threshold)
    return result[1]


def _get_filt(period):
    """the filter to extract trend component"""
    if period % 2 == 0:
        filt = np.array([.5] + [1] * (period - 1) + [.5]) / period
    else:
        filt = np.repeat(1. / period, period)
    return filt


def _get_seasonal(x_raw, detrended, period):
    """"return seasonal component from x_raw, detrended and period"""
    nobs = len(x_raw)
    period_averages = np.array([np.nanmean(detrended[i::period]) for i in range(period)])
    period_averages -= np.mean(period_averages, axis=0)
    seasonal = np.tile(period_averages.T, nobs // period + 1).T[:nobs]

    return seasonal


def seasonal_decompose(x_raw, period=None):
    """seasonal series can decompose three component: trend, seasonal, resid"""
    pfreq = period

    if np.ndim(x_raw) > 1:
        raise ValueError("x ndim > 1 not implemented")

    if period is None:
        raise ValueError("preiod must not None")

    if not np.all(np.isfinite(x_raw)):
        raise ValueError("the x has no valid values")

    if x_raw.shape[0] < 2 * pfreq:
        raise ValueError(f"the x length:{x_raw.shape[0]} not meet 2 preiod:{2 * pfreq}")
    x_raw = trim_head_and_tail_nan(x_raw)
    filt = _get_filt(period)
    trend = _get_trend(x_raw, filt)
    trend = trim_head_and_tail_nan(trend)
    detrended = x_raw - trend

    seasonal = _get_seasonal(x_raw, detrended, period)
    seasonal = trim_head_and_tail_nan(seasonal)
    resid = detrended - seasonal
    return seasonal, trend, resid
