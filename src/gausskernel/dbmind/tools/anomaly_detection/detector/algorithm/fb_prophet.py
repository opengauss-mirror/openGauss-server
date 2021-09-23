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
import pickle
import time

import pandas as pd
from fbprophet import Prophet

from .model import AlgModel


class FbProphet(AlgModel):
    """
    This class inherits from the AlgModel class.
    It is based on the Facebook prophet algorithm and uses to forecast time-series.
    """

    def __init__(self):
        AlgModel.__init__(self)
        self.model = None
        self.train_length = 0

    def fit(self, timeseries):
        """
        :param timeseries: list, it should include timestamp and value like
        [[111111111, 2222222222, ...], [4.0, 5.0, ...]].
        :return: NA
        """
        timeseries = pd.DataFrame(timeseries, columns=['ds', 'y'])
        timeseries['ds'] = timeseries['ds'].map(
            lambda x: time.strftime(AlgModel.DATE_FORMAT, time.localtime(x)))
        self.train_length = len(timeseries)
        self.model = Prophet(yearly_seasonality=True,
                             weekly_seasonality=True,
                             daily_seasonality=True)
        self.model.fit(timeseries)

    def forecast(self, period, freq):
        """
        :param freq: int, time interval.
        :param period: string, like '100S','1D', reprensent forecast period.
        :return: list, forecast result which include date and value.
        """
        if freq.endswith('M'):
            freq = freq.replace('M', 'T')

        future = self.model.make_future_dataframe(freq=freq,
                                                  periods=period,
                                                  include_history=False)
        forecast_result = self.model.predict(future)[['ds', 'yhat']]
        forecast_result['ds'] = forecast_result['ds'].map(lambda x: x.strftime(AlgModel.DATE_FORMAT))
        return forecast_result.values[:, 0], forecast_result.values[:, 1]

    def save(self, model_path):
        with open(model_path, mode='wb') as f:
            pickle.dump(self.model, f)

    def load(self, model_path):
        with open(model_path, mode='rb') as f:
            self.model = pickle.load(f)
