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

from ._abstract_detector import AbstractDetector
from ._utils import remove_edge_effect
from .agg import merge_with_and_operator
from .detector_params import THRESHOLD
from .iqr_detector import InterQuartileRangeDetector
from .threshold_detector import ThresholdDetector
from .. import stat_utils
from ..seasonal import seasonal_decompose, get_seasonal_period
from ...types import Sequence


class SeasonalDetector(AbstractDetector):
    def __init__(self, outliers=(3, 3), side="positive", window=10, period=None,
                 high_ac_threshold=0.1, min_seasonal_freq=2):
        self.outliers = outliers
        self.side = side
        self.window = window
        self.period = period
        self.high_ac_threshold = high_ac_threshold
        self.min_seasonal_freq = min_seasonal_freq

    def _fit(self, s: Sequence):
        if not self.period:
            self.period = get_seasonal_period(
                    s.values,
                    high_ac_threshold=self.high_ac_threshold,
                    min_seasonal_freq=self.min_seasonal_freq
            )

        self._iqr_detector = InterQuartileRangeDetector(outliers=self.outliers)
        self._sign_detector = ThresholdDetector(high=THRESHOLD.get(self.side)[0],
                                                low=THRESHOLD.get(self.side)[1])

    def _predict(self, s: Sequence) -> Sequence:
        moving_average = stat_utils.np_moving_avg(s.values, window=self.window)
        deseasonal_residual = seasonal_decompose(moving_average, period=self.period)[2]
        residual_sequence = Sequence(timestamps=s.timestamps, values=deseasonal_residual)

        iqr_result = self._iqr_detector.fit_predict(residual_sequence)  # iqr detection
        sign_check_result = self._sign_detector.fit_predict(residual_sequence)  # threshold detection
        res = merge_with_and_operator([iqr_result, sign_check_result])  # aggregation
        return remove_edge_effect(res, self.window)
