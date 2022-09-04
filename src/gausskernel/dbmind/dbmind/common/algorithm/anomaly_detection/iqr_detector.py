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

from ._abstract_detector import AbstractDetector
from .detector_params import EFFECTIVE_LENGTH
from .. import stat_utils
from ...types import Sequence


class InterQuartileRangeDetector(AbstractDetector):
    def __init__(self, outliers=(3, 3), sigma=0.05):
        self.outliers = outliers
        self.sigma = sigma

    def _fit(self, s: Sequence) -> None:
        q1 = np.nanpercentile(s.values, 25)
        q3 = np.nanpercentile(s.values, 75)
        iqr = q3 - q1

        if isinstance(self.outliers[0], (int, float)):
            self.lower_bound = (q1 - iqr * self.outliers[0])
            q0 = np.nanpercentile(s.values, 0)
            qx = np.nanpercentile(s.values, 3 * self.sigma)
            if self.lower_bound <= q0 or (len(s) >= EFFECTIVE_LENGTH and self.lower_bound > qx):
                self.lower_bound = qx
        else:
            self.lower_bound = -float("inf")

        if isinstance(self.outliers[1], (int, float)):
            self.upper_bound = (q3 + iqr * self.outliers[1])
            q4 = np.nanpercentile(s.values, 100)
            qx = np.nanpercentile(s.values, 100 - 3 * self.sigma)
            if self.upper_bound >= q4 or (len(s) >= EFFECTIVE_LENGTH and self.upper_bound < qx):
                self.upper_bound = qx
        else:
            self.upper_bound = float("inf")

    def _predict(self, s: Sequence) -> Sequence:
        values = np.array(s.values)
        predicted_values = (values > self.upper_bound) | (values < self.lower_bound)
        return Sequence(timestamps=s.timestamps, values=predicted_values)


def remove_spike(s: Sequence):
    abs_diff_values = stat_utils.np_double_rolling(s.values, window1=10, diff_mode="abs_diff")
    iqr_ad_sequence = InterQuartileRangeDetector(
        outliers=(None, 3)
    ).fit_predict(
        Sequence(s.timestamps, abs_diff_values)
    )

    timestamps, values = [], []
    for i, v in enumerate(iqr_ad_sequence.values):
        timestamps.append(s.timestamps[i])
        if not v:
            values.append(s.values[i])
        elif i > 0:
            values.append(s.values[i - 1])
        else:
            idx = iqr_ad_sequence.values.index(False)  # find the nearest element
            values.append(s.values[idx])
    return Sequence(timestamps=timestamps, values=values)
