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
from ..business_db import get_session
from ..schema import SlowQueries
from ._common import truncate_table

from sqlalchemy.orm import load_only


def insert_slow_query(
        schema_name, db_name, query, start_at, duration_time,
        template_id=None, hit_rate=None, fetch_rate=None,
        cpu_time=None, data_io_time=None, root_cause=None, suggestion=None
):
    with get_session() as session:
        session.add(
            SlowQueries(
                schema_name=schema_name,
                db_name=db_name,
                query=query,
                template_id=template_id,
                start_at=start_at,
                duration_time=duration_time,
                hit_rate=hit_rate,
                fetch_rate=fetch_rate,
                cpu_time=cpu_time,
                data_io_time=data_io_time,
                root_cause=root_cause,
                suggestion=suggestion
            )
        )


def select_slow_queries(target_list=(), query=None, start_time=None, end_time=None):
    with get_session() as session:
        result = session.query(SlowQueries)
        if len(target_list) > 0:
            result = result.options(load_only(*target_list))
        if query is not None:
            result = result.filter(
                SlowQueries.query.like(query)
            )
        if start_time is not None:
            result = result.filter(
                SlowQueries.start_at >= start_time
            )
        if end_time is not None:
            result = result.filter(
                SlowQueries.start_at <= end_time
            )

        return result.order_by(SlowQueries.start_at)


def count_slow_queries():
    with get_session() as session:
        return session.query(SlowQueries.slow_query_id).count()


def delete_slow_queries(start_time):
    """To prevent the table from over-expanding."""
    with get_session() as session:
        session.query(SlowQueries).filter(
            SlowQueries.start_at <= start_time
        ).delete()


def truncate_slow_queries():
    truncate_table(SlowQueries.__tablename__)
