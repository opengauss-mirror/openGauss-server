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
from collections import OrderedDict
from itertools import groupby

a_logger = logging.getLogger('abnormal')
m_logger = logging.getLogger('detector')


class Detector:
    """
    This class is used for detecting result of forecastor, if the result from forecastor 
    is beyond expectation, it can provide alarm function in log file.
    """

    @staticmethod
    def detect(forecast_result):
        def mapper_function(val):
            if val > maximum:
                return val, 'higher'
            elif val < minimum:
                return val, 'lower'
            else:
                return val, 'normal'

        if forecast_result['status'] == 'fail':
            return

        metric_name = forecast_result['metric_name']
        future_value = forecast_result['future_value']
        future_date = forecast_result['future_date']
        minimum = forecast_result['detect_basis']['minimum']
        maximum = forecast_result['detect_basis']['maximum']

        if minimum is None and maximum is not None:
            minimum = '-inf'
            value_map_result = list(map(lambda x: (x, 'higher') if x > maximum else (x, 'normal'), future_value))
        elif maximum is None and minimum is not None:
            maximum = 'inf'
            value_map_result = list(map(lambda x: (x, 'lower') if x < minimum else (x, 'normal'), future_value))
        else:
            value_map_result = list(map(mapper_function, future_value))
        forecast_condition = OrderedDict(zip(future_date, value_map_result))
        for key, value in groupby(list(forecast_condition.items()), key=lambda item: item[1][1]):
            metric_status = key
            metric_date_value_scope = [(item[0], item[1][0]) for item in value]
            maximum_forecast_value = round(max([item[1] for item in metric_date_value_scope]), 3)
            minimum_forecast_value = round(min([item[1] for item in metric_date_value_scope]), 3)
            if metric_status == 'normal':
                if len(metric_date_value_scope) == 1:
                    m_logger.info('The forecast value of [{metric}]({minimum}~{maximum})'
                                  ' at {date} is ({forecast_value})  [{metric_status}].'
                                  .format(metric=metric_name,
                                          minimum=minimum,
                                          maximum=maximum,
                                          forecast_value=metric_date_value_scope[0][1],
                                          metric_status=metric_status,
                                          date=metric_date_value_scope[0][0]))
                else:
                    m_logger.info('The forecast value of [{metric}]({minimum}~{maximum}) in '
                                  '[{start_date}~{end_date}] is between ({minimum_forecast_value}'
                                  '~{maximum_forecast_value})  [{metric_status}].'
                                  .format(metric=metric_name,
                                          minimum=minimum,
                                          maximum=maximum,
                                          minimum_forecast_value=minimum_forecast_value,
                                          maximum_forecast_value=maximum_forecast_value,
                                          metric_status=metric_status,
                                          start_date=metric_date_value_scope[0][0],
                                          end_date=metric_date_value_scope[-1][0]))
            else:
                if len(metric_date_value_scope) == 1:
                    a_logger.warning('The forecast value of [{metric}]({minimum}~{maximum})'
                                     ' at {date} is ({forecast_value})  [{metric_status}].'
                                     .format(metric=metric_name,
                                             minimum=minimum,
                                             maximum=maximum,
                                             forecast_value=metric_date_value_scope[0][1],
                                             metric_status=metric_status,
                                             date=metric_date_value_scope[0][0]))
                else:
                    a_logger.warning('The forecast value of [{metric}]({minimum}~{maximum}) in '
                                     '[{start_date}~{end_date}] is between ({minimum_forecast_value}'
                                     '~{maximum_forecast_value})  [{metric_status}].'
                                     .format(metric=metric_name,
                                             minimum=minimum,
                                             maximum=maximum,
                                             minimum_forecast_value=minimum_forecast_value,
                                             maximum_forecast_value=maximum_forecast_value,
                                             metric_status=metric_status,
                                             start_date=metric_date_value_scope[0][0],
                                             end_date=metric_date_value_scope[-1][0]))
