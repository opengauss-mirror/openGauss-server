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
import os
import pickle
import re
import time

import pandas as pd
from fbprophet import Prophet

from .model import AlgModel

date_format = "%Y-%m-%d %H:%M:%S"


class FacebookProphet(AlgModel):
    """
    This class inherit from AlgModel, it is based on facebook prophet algorithm
    and use to forecast timeseries.
    """

    def __init__(self):
        AlgModel.__init__(self)
        self.model = None
        self.date_unit_mapper = {'S': 'S',
                                 'M': 'T',
                                 'H': 'H',
                                 'D': 'D',
                                 'W': 'W'}

    def fit(self, timeseries):
        """
        :param timeseries: list, it should include timestamp and value like 
        [[111111111, 2222222222, ...], [4.0, 5.0, ...]].
        :return: NA
        """
        try:
            timeseries = pd.DataFrame(timeseries, columns=['ds', 'y'])
            timeseries['ds'] = timeseries['ds'].map(lambda x: time.strftime(date_format, time.localtime(x)))
            self.model = Prophet(yearly_seasonality=True,
                                 weekly_seasonality=True,
                                 daily_seasonality=True)
            self.model.fit(timeseries)
        except Exception:
            raise

    def forecast(self, forecast_periods):
        """
        :param forecast_periods: string, like '100S','1D', reprensent forecast period.
        :return: list, forecast result which include date and value.
        """
        forecast_period, forecast_freq = re.match(r'(\d+)?([WDHMS])', forecast_periods).groups()
        try:
            # synchronize date-unit to fb-prophet
            forecast_freq = self.date_unit_mapper[forecast_freq]
            if forecast_period is None:
                forecast_period = 1
            else:
                forecast_period = int(forecast_period)
            future = self.model.make_future_dataframe(freq=forecast_freq,
                                                      periods=forecast_period,
                                                      include_history=False)
            forecast_result = self.model.predict(future)[['ds', 'yhat']]
            forecast_result['ds'] = forecast_result['ds'].map(lambda x: x.strftime(date_format))
            return forecast_result.values[:, 0], forecast_result.values[:, 1]
        except Exception:
            raise

    def save(self, model_path):
        with open(model_path, mode='wb') as f:
            pickle.dump(self.model, f)

    def load(self, model_path):
        if not os.path.exists(model_path):
            raise FileNotFoundError('%s not found.' % model_path)
        with open(model_path, mode='rb') as f:
            self.model = pickle.load(f)
