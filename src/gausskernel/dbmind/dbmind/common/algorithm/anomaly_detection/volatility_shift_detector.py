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
from .iqr_detector import InterQuartileRangeDetector
from .. import stat_utils
from ...types import Sequence


class VolatilityShiftDetector(AbstractDetector):
    def __init__(self, outliers=(None, 3), window=5):
        self.outliers = outliers
        self.window = window

    def _fit(self, s: Sequence) -> None:
        self._iqr_detector = InterQuartileRangeDetector(outliers=self.outliers)

    def _predict(self, s: Sequence) -> Sequence:
        std = stat_utils.np_moving_std(s.values, self.window)
        iqr_result = self._iqr_detector.fit_predict(Sequence(s.timestamps, std))
        return iqr_result
