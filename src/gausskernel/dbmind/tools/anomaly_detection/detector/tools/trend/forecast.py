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
import logging
import sys
import os

from detector.tools.trend.detect import Detector

m_logger = logging.getLogger('detector')


class Forecaster:
    """
    This class is used for forecasting future trends for timeseries based on 
    timeseries forecast algorithm
    """

    def __init__(self, *args, **kwargs):
        self.minimum_timeseries_length = 4
        self.database_dir = kwargs['database_dir']
        self.forecast_alg = kwargs['forecast_alg']
        self.data_period = kwargs['data_period']
        self.interval = kwargs['interval']
        self.period = kwargs['period']
        self.freq = kwargs['freq']
        self.data_handler = kwargs['data_handler']
        self.trend_service_list = kwargs['trend_service_list']
        self.forecast_handler = kwargs['forecast_handler']
        self.metric_config = kwargs['metric_config']

    def run(self):
        for database in os.listdir(self.database_dir):
            with self.data_handler(os.path.join(self.database_dir, database)) as db:
                if 'journal' in database:
                    continue
                try:
                    tables = db.get_all_tables()
                    last_timestamp = None
                    for table in self.trend_service_list:
                        if table not in tables:
                            m_logger.warning("Table {table} is not in {database}.".format(table=table, database=database))
                            continue
                        fields = db.get_all_fields(table)
                        if not last_timestamp:
                            last_timestamp = db.get_latest_timestamp(table)
                        for field in fields:
                            forecast_result = {}
                            timeseries = db.get_timeseries(table=table, field=field, period=self.data_period,
                                                           timestamp=last_timestamp)
                            if not timeseries:
                                m_logger.error("Can not get time series from {table}-{field} by period '{period}', "
                                               "skipping forecast.".format(table=table, field=field,
                                                                           period=self.data_period))
                                forecast_result['status'] = 'fail'
                                continue

                            if len(timeseries) < self.minimum_timeseries_length:
                                m_logger.error(
                                    "The length of time series in {table}-{field} is too short: [{ts_length}], "
                                    "so you can adjust 'data_period'.".format(table=table, field=field,
                                                                              ts_length=len(timeseries)))
                                continue
                            self.forecast_handler.fit(timeseries)
                            date, value = self.forecast_handler.forecast(period=self.period, freq=self.freq)
                            try:
                                minimum = None if not self.metric_config.has_option(
                                    table, field + '_minimum') else self.metric_config.getfloat(
                                    table, field + '_minimum')
                                maximum = None if not self.metric_config.has_option(
                                    table, field + '_maximum') else self.metric_config.getfloat(
                                    table, field + '_maximum')
                            except Exception as e:
                                m_logger.error("{table} - {field}: {err}".format(table=table, field=field, err=str(e))) 
                                continue
                            if minimum is None and maximum is None:
                                m_logger.error("{table} - {field}: The minimum and maximum is not provided, you should at least provide one of it.".format(table=table, field=field))
                                continue
                            if minimum is not None and maximum is not None and minimum > maximum:
                                m_logger.error("{table} - {field}: The minimum is greater than the maximum.".format(table=table, field=field))
                                continue
                            detect_basis = {'minimum': minimum, 'maximum': maximum}
                            forecast_result['status'] = 'success'
                            forecast_result['metric_name'] = table + '-->' + field
                            forecast_result['detect_basis'] = detect_basis
                            forecast_result['future_date'] = date
                            forecast_result['future_value'] = value
                            Detector.detect(forecast_result)
                except Exception as e:
                    m_logger.error(str(e), exc_info=True)
                    sys.exit(-1)
