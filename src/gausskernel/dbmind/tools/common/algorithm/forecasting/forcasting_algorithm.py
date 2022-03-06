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

import logging
import numpy as np
import itertools
from types import SimpleNamespace
from ...types import Sequence
from ..statistics import sequence_interpolate, trim_head_and_tail_nan
from .. import seasonal as seasonal_interface


MAX_AR_ORDER = 5
MAX_MA_ORDER = 5
MIN_DATA_LENGTH = max(MAX_AR_ORDER, MAX_MA_ORDER)


def estimate_order_of_model_parameters(raw_data, k_ar_min=0, k_diff_min=0,
                                 k_ma_min=0, k_diff_max=0):
    """return model type and model order"""
    diff_data = np.diff(raw_data)
    algorithm_name = "linear"
    k_ar_valid, k_ma_valid = 0, 0
    min_bic = np.inf
    bic_result_list = []
    for k_ar, k_diff, k_ma in \
            itertools.product(range(k_ar_min, MAX_AR_ORDER + 1),
                              range(k_diff_min, k_diff_max + 1),
                              range(k_ma_min, MAX_MA_ORDER + 1)):
        if k_ar == 0 and k_diff == 0 and k_ma == 0:
            continue

        try:
            from .arima_model.arima_alg import ARIMA

            model = ARIMA(diff_data, order=(k_ar, k_diff, k_ma), )
            model.fit()
            bic_result = model.bic
            bic_result_list.append(bic_result)
            if not np.isnan(bic_result) and bic_result < min_bic:
                algorithm_name = "arima"
                min_bic = bic_result
                k_ar_valid = k_ar
                k_ma_valid = k_ma
        except ValueError:
            """Ignore while ValueError occurred."""
        except Exception as e:
            logging.warning("Warning occurred when estimate order of model parameters, "
                            "warning_msg is: %s", e)
    order = (k_ar_valid, 1, k_ma_valid)
    return algorithm_name, order


class ForecastingAlgorithm:
    """abstract forecast alg class"""

    def fit(self, sequence: Sequence):
        """the subclass should implement, tarin model param"""
        pass

    def forecast(self, forecast_length: int) -> Sequence:
        """the subclass should implement, forecast series according history series"""
        pass


class ForecastingFactory:
    """the ForecastingFactory can create forecast model"""
    _CACHE = {}  # Reuse an instantiated object.

    @staticmethod
    def get_instance(raw_data) -> ForecastingAlgorithm:
        """return forecast model according algorithm_name"""
        algorithm_name, order = estimate_order_of_model_parameters(raw_data)
        logging.debug('Choose %s algorithm to forecast.', algorithm_name)
        if algorithm_name == "linear":
            from .simple_forecasting import SimpleLinearFitting
            ForecastingFactory._CACHE[algorithm_name] = SimpleLinearFitting()
        elif algorithm_name == "arima" or algorithm_name is None:
            from .arima_model.arima_alg import ARIMA
            ForecastingFactory._CACHE[algorithm_name] = ARIMA(raw_data, order)
        else:
            raise NotImplementedError(f'Failed to load {algorithm_name} algorithm.')

        return ForecastingFactory._CACHE[algorithm_name]


def _check_forecasting_minutes(forecasting_minutes):
    """
    check input params: forecasting_minutes whether is valid.
    :param forecasting_minutes: type->int or float
    :return: None
    """
    check_result = True
    message = ""
    if not isinstance(forecasting_minutes, (int, float)):
        check_result = False
        message = "forecasting_minutes value type must be int or float"
    elif forecasting_minutes < 0:
        check_result = False
        message = "forecasting_minutes value must >= 0"
    elif forecasting_minutes in (np.inf, -np.inf, np.nan, None):
        check_result = False
        message = f"forecasting_minutes value must not be:{forecasting_minutes}"

    if not check_result:
        raise ValueError(message)


def decompose_sequence(sequence):
    seasonal_data = None
    raw_data = np.array(list(sequence.values))
    is_seasonal, period = seasonal_interface.is_seasonal_series(raw_data)
    if is_seasonal:
        decompose_results = seasonal_interface.seasonal_decompose(raw_data, period=period)
        seasonal = decompose_results[0]
        trend = decompose_results[1]
        resid = decompose_results[2]
        train_sequence = Sequence(timestamps=sequence.timestamps, values=trend)
        train_sequence = sequence_interpolate(train_sequence)
        seasonal_data = SimpleNamespace(is_seasonal=is_seasonal,
                                        seasonal=seasonal,
                                        trend=trend,
                                        resid=resid,
                                        period=period)
    else:
        train_sequence = sequence
    return seasonal_data, train_sequence


def compose_sequence(seasonal_data, train_sequence, forecast_length, forecast_data):
    if seasonal_data and seasonal_data.is_seasonal:
        start_index = len(train_sequence) % seasonal_data.period
        forecast_data = seasonal_data.seasonal[start_index: start_index + forecast_length] + \
                        forecast_data + \
                        seasonal_data.resid[start_index: start_index + forecast_length]
    forecast_timestamps = [train_sequence.timestamps[-1] + train_sequence.step * (i + 1)
                           for i in range(int(forecast_length))]
    return Sequence(timestamps=forecast_timestamps, values=forecast_data)


def quickly_forecast(sequence, forecasting_minutes):
    """
    return forecast sequence in forecasting_minutes from raw sequnece
    :param sequence: type->Sequence
    :param forecasting_minutes: type->int or float
    :return: forecase sequence: type->Sequence
    """
    # 1 check forecasting minutes
    _check_forecasting_minutes(forecasting_minutes)
    forecasting_length = int(forecasting_minutes * 60 * 1000 // sequence.step)
    if forecasting_length == 0 or forecasting_minutes == 0:
        return Sequence()

    # 2 interpolate
    sequence = sequence_interpolate(sequence)

    # 3 decompose sequence
    seasonal_data, train_sequence = decompose_sequence(sequence)

    # 4 get model from ForecastingFactory
    model = ForecastingFactory.get_instance(list(train_sequence.values))

    # 5 model fit and forecast
    model.fit(train_sequence)
    forecast_data = model.forecast(forecasting_length)
    forecast_data = trim_head_and_tail_nan(forecast_data)

    # 6 compose sequence
    forecast_sequence = compose_sequence(seasonal_data,
                                         train_sequence,
                                         forecasting_length,
                                         forecast_data)
    return forecast_sequence
