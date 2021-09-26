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

import time

import pandas as pd
import pmdarima as pm

from .model import AlgModel


class AutoArima(AlgModel):
    def __init__(self):
        AlgModel.__init__(self)
        self.model = None
        self.end_date = None

    def fit(self, timeseries):
        timeseries = pd.DataFrame(timeseries, columns=['ds', 'y'])
        timeseries['ds'] = timeseries['ds'].map(lambda x: time.strftime(AlgModel.DATE_FORMAT, time.localtime(x)))
        timeseries.set_index(['ds'], inplace=True)
        timeseries.index = pd.to_datetime(timeseries.index)
        self.end_date = timeseries.index[-1]
        self.model = pm.auto_arima(timeseries, seasonal=True)
        self.model.fit(timeseries)

    def forecast(self, period, freq):
        if freq.endswith('M'):
            freq = freq.replace('M', 'T')

        forecast_date_range = pd.date_range(start=self.end_date,
                                            periods=period + 1,
                                            freq=freq,
                                            closed='right')
        forecast_date_range = forecast_date_range.map(
            lambda x: x.strftime(AlgModel.DATE_FORMAT)
        ).values
        forecast_value = self.model.predict(period)
        return forecast_date_range, forecast_value
