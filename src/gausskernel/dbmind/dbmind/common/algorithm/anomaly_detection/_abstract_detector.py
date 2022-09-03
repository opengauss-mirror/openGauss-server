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
from abc import abstractmethod

from ._utils import pick_out_anomalies
from ...types import Sequence


class AbstractDetector(object):
    @abstractmethod
    def _fit(self, sequence: Sequence):
        pass

    @abstractmethod
    def _predict(self, sequence: Sequence) -> Sequence:
        pass

    def fit_predict(self, sequence: Sequence):
        self._fit(sequence)
        anomalies = self._predict(sequence)
        return anomalies

    def detect(self, sequence: Sequence):
        anomalies = self.fit_predict(sequence)
        return pick_out_anomalies(sequence, anomalies)
