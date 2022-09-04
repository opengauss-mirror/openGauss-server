# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

from ._common import truncate_table
from ..business_db import get_session
from ..schema import KnobRecommendationDetails
from ..schema import KnobRecommendationMetricSnapshot
from ..schema import KnobRecommendationWarnings


def truncate_knob_recommend_tables():
    truncate_table(KnobRecommendationDetails.__tablename__)
    truncate_table(KnobRecommendationWarnings.__tablename__)
    truncate_table(KnobRecommendationMetricSnapshot.__tablename__)


def insert_knob_recommend(host,
                          name,
                          current: str,
                          recommend,
                          min_,
                          max_,
                          restart
                          ):
    """insert knob recommend into the table."""
    with get_session() as session:
        session.add(
            KnobRecommendationDetails(
                host=host,
                name=name,
                current=current,
                recommend=recommend,
                min=min_,
                max=max_,
                restart=restart,
            )
        )


def batch_insert_knob_metric_snapshot(host, metric_dict):
    """batch insert knob recommend metric snapshot into the table."""
    metric_lists = []
    for metric_name, metric_value in metric_dict.items():
        # Check for NaN and None.
        if metric_value is None or metric_value != metric_value:
            logging.warning("batch_insert_knob_metric_snapshot: the value of %s is invalid, "
                            "so skipping the insertion." % metric_name)
            continue
        metric_lists.append(
            KnobRecommendationMetricSnapshot(
                host=host,
                metric=metric_name,
                value=metric_value
            )
        )
    with get_session() as session:
        session.bulk_save_objects(metric_lists)


def batch_insert_knob_recommend_warnings(host, warn, bad):
    """insert knob recommend warnings into the table."""
    warning_list = []
    for line in warn:
        warning_list.append(
            KnobRecommendationWarnings(
                host=host,
                comment=line,
                level="WARN"
            )
        )

    for line in bad:
        warning_list.append(
            KnobRecommendationWarnings(
                host=host,
                comment=line,
                level="BAD"
            )
        )

    with get_session() as session:
        session.bulk_save_objects(warning_list)


def select_metric_snapshot():
    with get_session() as session:
        return session.query(KnobRecommendationMetricSnapshot)


def select_warnings():
    with get_session() as session:
        return session.query(KnobRecommendationWarnings)


def select_details():
    with get_session() as session:
        return session.query(KnobRecommendationDetails)
