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

from ._abstract_detector import AbstractDetector
from ._utils import merge_contiguous_anomalies_timestamps, over_max_coef
from .iqr_detector import remove_spike
from ...types import Sequence


def linear_fitting(values, value_mean=1):
    if abs(value_mean) < 1:
        value_mean = 1

    axis_x = np.arange(1, 1 + len(values), 1)
    axis_y = np.array(values, dtype='float')
    axis_y /= abs(value_mean)
    coef, intercept = np.polyfit(axis_x, axis_y, deg=1)
    return coef, intercept


def is_stable(values, stable_threshold=0.15):
    p0 = np.nanpercentile(values, 0)
    p1 = np.nanpercentile(values, 25)
    p3 = np.nanpercentile(values, 75)
    p4 = np.nanpercentile(values, 100)
    return (p3 - p1) / (p4 - p0) <= stable_threshold


gsfm_range = SimpleNamespace(samping_rate=0.2, step=5, stable_threshold=0.15)


def get_stable_fragment_mean(values, gsfm_range_):
    """the stable_fragment_mean is used for zooming the size of data,
    for example: values /= stable_fragment_mean
    if no stable fragment found, return the average of the whole dataset.
    """
    length = len(values)
    window = int(length * gsfm_range_.samping_rate)
    for i in range(0, length, gsfm_range_.step):
        fragment = values[i:i + window]
        if is_stable(fragment, stable_threshold=gsfm_range_.stable_threshold):
            stable_fragment_mean = np.mean(fragment)
            return stable_fragment_mean

    return np.mean(values)


def increase_condition(values, side, max_increase_rate):
    length = len(values)
    values = np.array(values)
    if side == "positive":
        increase_rate = (values[1:] - values[:-1] > 0).sum() / length
    elif side == "negative":
        increase_rate = (values[1:] - values[:-1] < 0).sum() / length
    else:
        raise ValueError(side)

    return increase_rate, increase_rate > max_increase_rate


def over_max_trend_count(predicted, max_trend_count):
    length = len(predicted)
    anomalies_timestamps = [predicted.timestamps[i] for i in range(length) if predicted.values[i]]
    anomalies_timestamps_list = merge_contiguous_anomalies_timestamps(
        predicted.timestamps, anomalies_timestamps
    )
    return len(anomalies_timestamps_list) > max_trend_count


class IncreaseDetector(AbstractDetector):
    def __init__(self, side="positive", window=20, max_coef=0.3, max_increase_rate=0.7,
                 max_trend_count=5, gsfm_range_=gsfm_range):
        self.side = side
        self.window = window
        self.max_coef = max_coef
        self.max_increase_rate = max_increase_rate
        self.max_trend_count = max_trend_count
        self.gsfm_range = gsfm_range_

    def do_increase_detect(self, s: Sequence, predicted):
        """ We only need one stable fragment."""
        length = len(s)
        stable_fragment_mean = get_stable_fragment_mean(s.values, gsfm_range)

        for i in range(0, length, self.window):
            fragment = s.values[i: i + self.window]
            increase_rate, over_increase_rate = increase_condition(fragment, self.side, self.max_increase_rate)
            coef, _ = linear_fitting(fragment, stable_fragment_mean)
            if over_increase_rate or over_max_coef(coef, self.side, self.max_coef):
                predicted[i: i + self.window] = [True] * self.window

        return predicted

    def _fit(self, sequence: Sequence):
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        sequence_len = len(s)
        self.window = min(self.window, sequence_len)
        normal_sequence = remove_spike(s)  # Polish later: moving average

        predicted = [False] * sequence_len
        predicted = self.do_increase_detect(normal_sequence, predicted)

        predicted = Sequence(timestamps=s.timestamps, values=predicted)
        if over_max_trend_count(predicted, self.max_trend_count):
            predicted = Sequence(timestamps=s.timestamps, values=[False] * sequence_len)

        return predicted
