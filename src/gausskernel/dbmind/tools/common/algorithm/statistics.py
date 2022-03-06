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
from scipy.interpolate import interp1d
from dbmind.common.types import Sequence


def np_quantile(values, quantile):
    """return the quantile of values"""
    return np.nanpercentile(values, quantile)


def np_shift(values, shift_distance=1):
    """shift values a shift_distance"""
    shifted_values = np.roll(values, shift_distance)
    for i in range(shift_distance):
        shifted_values[i] = shifted_values[shift_distance]
    return shifted_values


def np_moving_avg(values, window=5, mode="same"):
    """Compute the moving average for sequence
    and create a new sequence as the return value."""
    moving_avg_values = np.convolve(values, np.ones((window,)) / window, mode=mode)
    start_idx = len(values) - window
    moving_avg_values[start_idx:] = moving_avg_values[start_idx]  # padding other remaining value
    return moving_avg_values


def np_moving_std(values, window=10):
    """Compute and return the standard deviation for sequence."""
    sequence_length = len(values)
    calculation_length = sequence_length - window
    moving_std_values = [np.std(values[i:i + window]) for i in range(calculation_length)]
    # padding
    for _ in range(window):
        moving_std_values.append(moving_std_values[-1])

    return np.array(moving_std_values)


def np_double_rolling(values, agg="mean", window1=5, window2=1, diff_mode="diff"):
    """double rolling the values"""
    if agg == "mean":
        left_rolling = np_moving_avg(np_shift(values), window=window1)
        right_rolling = np_moving_avg(values[::-1], window=window2)[::-1]
    elif agg == "std":
        left_rolling = np_moving_std(np_shift(values), window=window1)
        right_rolling = np_moving_std(values[::-1], window=window2)[::-1]
    else:
        return values
    diff_mode_map = {
        "diff": (right_rolling - left_rolling),
        "abs_diff": np.abs(right_rolling - left_rolling),
        "rel_diff": (right_rolling - left_rolling) / left_rolling,
        "abs_rel_diff": np.abs(right_rolling - left_rolling) / left_rolling
    }
    r_data = diff_mode_map.get(diff_mode)
    values_length = len(values)
    window = max(window1, window2)
    tail_length = int(window / 2)
    for i in range(tail_length):
        r_data[values_length - i - 1] = r_data[values_length - tail_length - 1]
    return r_data


def trim_head_and_tail_nan(data):
    """
    when there are nan value at head or tail of forecast_data,
    this function will fill value with near value
    :param data: type->np.array
    :return:data: type->np.array
    """
    head_start_nona_value = 0
    head_na_index = []
    tail_start_nona_value = 0
    tail_na_index = []

    if len(data) == 0:
        return data

    for i in range(len(data)):
        if not np.isnan(data[0]):
            break
        if not np.isnan(data[i]):
            head_start_nona_value = data[i]
            break
        else:
            head_na_index.append(i)

    for i in range(len(data) - 1, 1, -1):
        if not np.isnan(data[-1]):
            break
        if not np.isnan(data[i]):
            tail_start_nona_value = data[i]
            break
        else:
            tail_na_index.append(i)

    for i in head_na_index:
        data[i] = head_start_nona_value

    for i in tail_na_index:
        data[i] = tail_start_nona_value

    return data


def _init_interpolate_param(sequence):
    """"init interpolate param for sequence_interpolate function"""
    x_raw = np.array(list(range(len(sequence.timestamps))))
    y_raw = np.array(sequence.values)
    head_na_index = []
    head_start_nona_value = None
    tail_na_index = []
    tail_start_nona_value = None
    x_new = list(x_raw)
    y_new = list(y_raw)

    #init head_start_nona_value, head_na_index
    for i in range(len(y_raw)):
        if not np.isnan(y_raw[0]):
            break
        if not np.isnan(y_raw[i]):
            head_start_nona_value = y_raw[i]
            break
        else:
            head_na_index.append(i)

    #init tail_start_nona_value, tail_na_index
    for i in range(len(y_raw) - 1, 1, -1):
        if not np.isnan(y_raw[-1]):
            break
        if not np.isnan(y_raw[i]):
            tail_start_nona_value = y_raw[i]
            break
        else:
            tail_na_index.append(i)

    #pop the nan from head and tail of data
    for i in range(len(head_na_index)):
        x_new.pop(0)
        y_new.pop(0)

    for i in range(len(tail_na_index)):
        x_new.pop(-1)
        y_new.pop(-1)

    na_param = SimpleNamespace(head_na_index=head_na_index, tail_na_index=tail_na_index,
                               head_start_nona_value=head_start_nona_value,
                               tail_start_nona_value=tail_start_nona_value)
    return x_new, y_new, na_param


def sequence_interpolate(sequence: Sequence, fit_method="cubic"):
    """interpolate with scipy interp1d"""
    nan_exist_result = [True if not np.isnan(i) else False for i in sequence.values]
    if all(nan_exist_result):
        return sequence
    if True not in nan_exist_result:
        raise ValueError("sequence values are all nan")

    y_raw = np.array(sequence.values)
    y_nona = []
    x_nona = []
    na_index = []

    x_new, y_new, na_param = _init_interpolate_param(sequence)

    #prepare x_nona and y_nona for interp1d
    for i in range(len(y_new)):
        if not np.isnan(y_new[i]):
            y_nona.append(y_new[i])
            x_nona.append(x_new[i])
        else:
            na_index.append(i)

    fit_func = interp1d(x_nona, y_nona, kind=fit_method)
    y_new = fit_func(x_new)

    #replace the nan with interp1d value for raw y
    for i in na_index:
        raw_index = i + len(na_param.head_na_index)
        y_raw[raw_index] = y_new[i]

    y_raw[na_param.head_na_index] = na_param.head_start_nona_value
    y_raw[na_param.tail_na_index] = na_param.tail_start_nona_value
    return Sequence(timestamps=sequence.timestamps, values=y_raw)
