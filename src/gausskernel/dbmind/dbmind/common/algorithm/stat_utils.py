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


def double_padding(values, window):
    left_idx = window - 1 - (window - 1) // 2
    right_idx = len(values) - 1 - (window - 1) // 2
    values[:left_idx] = values[left_idx]  # padding left
    values[right_idx + 1:] = values[right_idx]  # padding right
    return values


def np_shift(values, shift_distance=1):
    """shift values a shift_distance"""
    if len(values) < 2:
        return values
    shifted_values = np.roll(values, shift_distance)
    for i in range(shift_distance):
        shifted_values[i] = shifted_values[shift_distance]
    return shifted_values


def np_moving_avg(values, window=5, mode="same"):
    """Computes the moving average for sequence
    and returns a new sequence padded with valid
    value at both ends.
    """
    moving_avg_values = np.convolve(values, np.ones((window,)) / window, mode=mode)
    moving_avg_values = double_padding(moving_avg_values, window)
    return moving_avg_values


def np_moving_std(values, window=10):
    """Computes the standard deviation for sequence
    and returns a new sequence padded with valid
    value at both ends.
    """
    sequence_length = len(values)
    moving_std_values = np.zeros(sequence_length)
    left_idx = window - 1 - (window - 1) // 2
    for i in range(sequence_length - window + 1):
        moving_std_values[left_idx + i] = np.std(values[i:i + window])

    moving_std_values = double_padding(moving_std_values, window)
    return moving_std_values


def np_double_rolling(values, window1=5, window2=1, diff_mode="diff"):
    values_length = len(values)
    window1 = 1 if values_length < window1 else window1
    window2 = 1 if values_length < window2 else window2

    left_rolling = np_moving_avg(np_shift(values), window=window1)
    right_rolling = np_moving_avg(values[::-1], window=window2)[::-1]
    r_data = right_rolling - left_rolling

    functions = {
        'abs': lambda x: np.abs(x),
        'rel': lambda x: x / left_rolling
    }
    methods = diff_mode.split('_')[:-1]
    for method in methods:
        r_data = functions[method](r_data)

    r_data = double_padding(r_data, max(window1, window2))
    return r_data


def measure_head_and_tail_nan(data):
    data_not_nan = -1 * np.isnan(data)
    left = data_not_nan.argmax()
    right = data_not_nan[::-1].argmax()
    return left, right


def trim_head_and_tail_nan(data):
    """
    when there are nan value at head or tail of forecast_data,
    this function will fill value with near value
    :param data: type->np.array or list
    :return data: type->same type as the input 'data'
    """
    length = len(data)
    if length == 0:
        return data

    data_not_nan = np.isnan(data)
    if data_not_nan.all():
        data[:] = [0] * length
        return data

    left, right = measure_head_and_tail_nan(data)

    data[:left] = [data[left]] * left
    data[length - right:] = [data[length - right - 1]] * right
    return data


def _valid_value(v):
    return not (np.isnan(v) or np.isinf(v))


def _init_interpolate_param(sequence):
    """"init interpolate param for sequence_interpolate function"""
    length = len(sequence)
    if length == 0:
        return sequence

    x = np.array(range(len(sequence)))
    y = np.array(sequence.values)
    left, right = measure_head_and_tail_nan(y)
    na_param = SimpleNamespace(head_na_index=range(left), tail_na_index=range(length - right, length),
                               head_start_nona_value=y[left],
                               tail_start_nona_value=y[length - right - 1])
    return x[left:length - right], y[left:length - right], na_param


def tidy_up_sequence(sequence):
    """Fill up missing values for sequence and
    align sequence's timestamps.
    """
    if sequence.step <= 0:
        return sequence

    def estimate_error(a, b):
        return (a - b) / b

    timestamps = list(sequence.timestamps)
    values = list(sequence.values)

    i = 1
    while i < len(timestamps):
        real_interval = timestamps[i] - timestamps[i - 1]
        error = estimate_error(real_interval, sequence.step)
        if error < 0:
            # This is because the current timestamp is lesser than the previous one.
            # We should remove one to keep monotonic.
            if not _valid_value(values[i - 1]):
                values[i - 1] = values[i]
            timestamps.pop(i)
            values.pop(i)
            i -= 1  # We have removed an element so we have to decrease the cursor.
        elif error == 0:
            """Everything is normal, skipping."""
        elif 0 < error < 1:
            # Align the current timestamp.
            timestamps[i] = timestamps[i - 1] + sequence.step
        else:
            # Fill up missing value with NaN.
            next_ = timestamps[i - 1] + sequence.step
            timestamps.insert(i, next_)
            values.insert(i, float('nan'))
        i += 1

    return Sequence(timestamps, values)


def sequence_interpolate(sequence: Sequence, fit_method="cubic", strip_details=True):
    """interpolate with scipy interp1d"""
    filled_sequence = tidy_up_sequence(sequence)
    has_defined = [_valid_value(v) for v in filled_sequence.values]

    if all(has_defined):
        if strip_details:
            return filled_sequence
        else:
            return Sequence(
                timestamps=filled_sequence.timestamps,
                values=filled_sequence.values,
                name=sequence.name,
                step=sequence.step,
                labels=sequence.labels
            )

    if True not in has_defined:
        raise ValueError("All of sequence values are undefined.")

    y_raw = np.array(filled_sequence.values)
    y_nona = []
    x_nona = []
    na_index = []

    x_new, y_new, na_param = _init_interpolate_param(filled_sequence)

    # prepare x_nona and y_nona for interp1d
    for i in range(len(y_new)):
        if _valid_value(y_new[i]):
            y_nona.append(y_new[i])
            x_nona.append(x_new[i])
        else:
            na_index.append(i)

    fit_func = interp1d(x_nona, y_nona, kind=fit_method)
    y_new = fit_func(x_new)

    # replace the nan with interp1d value for raw y
    for i in na_index:
        raw_index = i + len(na_param.head_na_index)
        y_raw[raw_index] = y_new[i]

    y_raw[na_param.head_na_index] = na_param.head_start_nona_value
    y_raw[na_param.tail_na_index] = na_param.tail_start_nona_value
    if strip_details:
        return Sequence(timestamps=filled_sequence.timestamps, values=y_raw)
    else:
        return Sequence(
            timestamps=filled_sequence.timestamps,
            values=y_raw,
            name=sequence.name,
            step=sequence.step,
            labels=sequence.labels
        )
