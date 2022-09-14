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
import threading
from types import SimpleNamespace
from typing import Union, List

import numpy as np

from dbmind.common.utils import dbmind_assert
from .. import seasonal as seasonal_interface
from ..stat_utils import sequence_interpolate, trim_head_and_tail_nan
from ...types import Sequence

LINEAR_THRESHOLD = 0.9


class ForecastingAlgorithm:
    """abstract forecast alg class"""

    def fit(self, sequence: Sequence):
        """the subclass should implement, tarin model param"""
        pass

    def forecast(self, forecast_length: int) -> Union[List, np.array]:
        """the subclass should implement, forecast series according history series"""
        pass


class ForecastingFactory:
    """the ForecastingFactory can create forecast model"""
    _CACHE = threading.local()  # Reuse an instantiated object.

    @staticmethod
    def _get(algorithm_name):
        if not hasattr(ForecastingFactory._CACHE, algorithm_name):
            if algorithm_name == 'linear':
                from .simple_forecasting import SimpleLinearFitting
                setattr(ForecastingFactory._CACHE, algorithm_name, SimpleLinearFitting(avoid_repetitive_fitting=True))
            elif algorithm_name == 'arima':
                from .arima_model.arima_alg import ARIMA
                setattr(ForecastingFactory._CACHE, algorithm_name, ARIMA())
            else:
                raise NotImplementedError(f'Failed to load {algorithm_name} algorithm.')

        return getattr(ForecastingFactory._CACHE, algorithm_name)

    @staticmethod
    def get_instance(sequence) -> ForecastingAlgorithm:
        """Return a forecast model according to the feature of given sequence."""
        linear = ForecastingFactory._get('linear')
        linear.refit()
        linear.fit(sequence)
        if linear.r2_score >= LINEAR_THRESHOLD:
            logging.debug('Choose linear fitting algorithm to forecast.')
            return linear
        logging.debug('Choose ARIMA algorithm to forecast.')
        return ForecastingFactory._get('arima')


def _check_forecasting_minutes(forecasting_minutes):
    """
    check whether input params forecasting_minutes is valid.
    :param forecasting_minutes: int or float
    :return: None
    :exception: raise ValueError if given parameter is invalid.
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
    raw_data = np.array(sequence.values)
    is_seasonal, period = seasonal_interface.is_seasonal_series(
        raw_data,
        high_ac_threshold=0.1,
        min_seasonal_freq=2
    )
    if is_seasonal:
        seasonal, trend, residual = seasonal_interface.seasonal_decompose(raw_data, period=period)
        train_sequence = Sequence(timestamps=sequence.timestamps, values=trend)
        train_sequence = sequence_interpolate(train_sequence)
        seasonal_data = SimpleNamespace(
            is_seasonal=is_seasonal,
            seasonal=seasonal,
            trend=trend,
            resid=residual,
            period=period
        )
    else:
        train_sequence = sequence
    return seasonal_data, train_sequence


def compose_sequence(seasonal_data, train_sequence, forecast_values):
    forecast_length = len(forecast_values)
    if seasonal_data and seasonal_data.is_seasonal:
        start_index = len(train_sequence) % seasonal_data.period
        seasonal = seasonal_data.seasonal
        resid = seasonal_data.resid
        dbmind_assert(len(seasonal) == len(resid))

        seasonal, resid = seasonal[start_index:], resid[start_index:]
        if len(seasonal) < forecast_length:  # pad it.
            padding_length = forecast_length - len(seasonal)
            seasonal = np.pad(seasonal, (0, padding_length), mode='wrap')
            resid = np.pad(resid, (0, padding_length), mode='wrap')
        else:
            seasonal, resid = seasonal[:forecast_length], resid[:forecast_length]

        resid[np.abs(resid - np.mean(resid)) > np.std(resid) * 3] = np.mean(resid)
        forecast_values = seasonal + forecast_values + resid

    forecast_timestamps = [train_sequence.timestamps[-1] + train_sequence.step * i
                           for i in range(1, forecast_length + 1)]
    return forecast_timestamps, forecast_values


def quickly_forecast(sequence, forecasting_minutes, lower=0, upper=float('inf')):
    """
    Return forecast sequence in forecasting_minutes from raw sequence.
    :param sequence: type->Sequence
    :param forecasting_minutes: type->int or float
    :param lower: The lower limit of the forecast result
    :param upper: The upper limit of the forecast result.
    :return: forecast sequence: type->Sequence
    """

    if len(sequence) <= 1:
        return Sequence()

    # 1. check for forecasting minutes
    _check_forecasting_minutes(forecasting_minutes)
    forecasting_length = int(forecasting_minutes * 60 * 1000 / sequence.step)
    if forecasting_length == 0 or forecasting_minutes == 0:
        return Sequence()

    # 2. interpolate
    interpolated_sequence = sequence_interpolate(sequence)

    # 3. decompose sequence
    seasonal_data, train_sequence = decompose_sequence(interpolated_sequence)

    # 4. get model from ForecastingFactory
    model = ForecastingFactory.get_instance(train_sequence)

    # 5. fit and forecast
    model.fit(train_sequence)

    forecast_data = model.forecast(forecasting_length)
    forecast_data = trim_head_and_tail_nan(forecast_data)
    dbmind_assert(len(forecast_data) == forecasting_length)

    # 6. compose sequence
    forecast_timestamps, forecast_values = compose_sequence(
        seasonal_data,
        train_sequence,
        forecast_data
    )

    for i in range(len(forecast_values)):
        forecast_values[i] = min(forecast_values[i], upper)
        forecast_values[i] = max(forecast_values[i], lower)

    return Sequence(
        timestamps=forecast_timestamps,
        values=forecast_values,
        name=sequence.name,
        labels=sequence.labels
    )
