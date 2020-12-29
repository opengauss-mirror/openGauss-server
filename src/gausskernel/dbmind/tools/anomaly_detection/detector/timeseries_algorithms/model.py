"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
from abc import abstractmethod


class AlgModel(object):
    """
    This is father class for forecast algorithm, if we want to 
    use your own forecast algorithm, we should follow some rules.
    """

    def __init__(self):
        pass

    @abstractmethod
    def fit(self, timeseries):
        pass

    @abstractmethod
    def forecast(self, forecast_periods):
        pass

    def save(self, model_path):
        pass

    def load(self, model_path):
        pass
