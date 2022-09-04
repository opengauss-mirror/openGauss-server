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
from ._common import truncate_table
from ..business_db import get_session
from ..schema import StatisticalMetric


def insert_record(metric_name, host, date, avg_val, min_val=0, max_val=0, the_95_quantile=0):
    with get_session() as session:
        session.add(
            StatisticalMetric(
                metric_name=metric_name,
                host=host,
                date=date,
                avg=avg_val,
                min=min_val,
                max=max_val,
                the_95_quantile=the_95_quantile,
            )
        )


def truncate():
    truncate_table(StatisticalMetric.__tablename__)


def count_records():
    with get_session() as session:
        result = session.query(StatisticalMetric)
        return result.count()


def select_knob_statistic_records():
    with get_session() as session:
        result = session.query(StatisticalMetric)
        return result
