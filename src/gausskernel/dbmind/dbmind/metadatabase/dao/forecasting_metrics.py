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
from typing import Sequence

from sqlalchemy import func, text

from ._common import truncate_table
from ..business_db import get_session
from ..schema import ForecastingMetrics


def delete_forecasting_metrics(metric_name, host, metric_min_time, metric_max_time):
    """Delete old forecast metric data."""
    with get_session() as session:
        session.query(ForecastingMetrics).filter(
            ForecastingMetrics.metric_name == metric_name,
            ForecastingMetrics.host == host,
            ForecastingMetrics.metric_time >= metric_min_time,
            ForecastingMetrics.metric_time <= metric_max_time
        ).delete()


def truncate_forecasting_metrics():
    truncate_table(ForecastingMetrics.__tablename__)


def batch_insert_forecasting_metric(metric_name, host,
                                    metric_value: Sequence, metric_time: Sequence,
                                    labels: str = None):
    """Batch insert node metrics into the table."""
    node_metric_lists = []
    for v, t in zip(metric_value, metric_time):
        node_metric_lists.append(
            ForecastingMetrics(
                metric_name=metric_name,
                host=host,
                metric_value=round(v, 2),
                metric_time=t,
                labels=labels
            )
        )
    with get_session() as session:
        session.bulk_save_objects(node_metric_lists)


def delete_timeout_forecasting_metrics(oldest_metric_time):
    """To prevent the table from over-expanding."""
    with get_session() as session:
        session.query(ForecastingMetrics).filter(
            ForecastingMetrics.metric_time < oldest_metric_time
        ).delete()


def aggregate_forecasting_metric(metric_name=None):
    with get_session() as session:
        result = session.query(ForecastingMetrics.metric_name,
                               func.min(ForecastingMetrics.metric_time).label('min'),
                               func.max(ForecastingMetrics.metric_time).label('max'),
                               ForecastingMetrics.host)
        if metric_name is not None:
            result = result.filter(
                ForecastingMetrics.metric_name == metric_name
            )

        result = result.group_by(
            ForecastingMetrics.host,
            ForecastingMetrics.metric_name
        )
        return result.order_by(text('min DESC'))


def select_forecasting_metric(
        metric_name=None, host=None,
        min_metric_time=None, max_metric_time=None,
        node_id=None
):
    with get_session() as session:
        result = session.query(ForecastingMetrics)
        if metric_name:
            result = result.filter(
                ForecastingMetrics.metric_name == metric_name
            )
        if host:
            result = result.filter(
                ForecastingMetrics.host == host
            )
        if min_metric_time:
            result = result.filter(
                min_metric_time <= ForecastingMetrics.metric_time
            )
        if max_metric_time:
            result = result.filter(
                ForecastingMetrics.metric_time <= max_metric_time
            )
        if node_id:
            result = result.filter(
                ForecastingMetrics.node_id == node_id
            )

        return result.order_by(ForecastingMetrics.metric_time)


def count_forecasting_metric(metric_name=None, host=None, node_id=None):
    with get_session() as session:
        result = session.query(ForecastingMetrics)
        if metric_name:
            result = result.filter(
                ForecastingMetrics.metric_name == metric_name
            )
        if host:
            result = result.filter(
                ForecastingMetrics.host == host
            )
        if node_id:
            result = result.filter(
                ForecastingMetrics.node_id == node_id
            )

        return result.count()
