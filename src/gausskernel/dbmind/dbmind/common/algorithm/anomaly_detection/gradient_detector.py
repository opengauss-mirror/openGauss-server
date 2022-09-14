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

import bisect

import numpy as np

from ._abstract_detector import AbstractDetector
from ._utils import over_max_coef
from .iqr_detector import remove_spike
from ...types import Sequence


def linear_fitting(x, y):
    x = np.array(x)
    y = np.array(y, dtype='float')
    coef, intercept = np.polyfit(x, y, deg=1)
    return coef, intercept


def sequence_partition(s: Sequence, timed_window: int):
    if s.timestamps[-1] - s.timestamps[0] < timed_window:
        return s, 0
    idx = bisect.bisect_right(s.timestamps, s.timestamps[-1] - timed_window)
    tail = Sequence(timestamps=s.timestamps[idx:], values=s.values[idx:])
    return tail, idx


class GradientDetector(AbstractDetector):
    def __init__(self, side="positive", max_coef=1, timed_window=300000):  # 300000 ms
        self.side = side
        self.max_coef = max_coef
        self.timed_window = timed_window

    def do_gradient_detect(self, s: Sequence):
        coef, _ = linear_fitting(s.timestamps, s.values)
        return (True,) * len(s) if over_max_coef(coef, self.side, self.max_coef) else (False,) * len(s)

    def _fit(self, sequence: Sequence):
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        normal_sequence = remove_spike(s)  # remove spike points
        tail, idx = sequence_partition(normal_sequence, self.timed_window)
        predicted = self.do_gradient_detect(tail)  # do detect for rapid change
        values = (False,) * idx + predicted
        return Sequence(timestamps=normal_sequence.timestamps, values=values)
