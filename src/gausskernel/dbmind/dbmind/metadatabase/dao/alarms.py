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
from sqlalchemy import update, desc, func

from ._common import truncate_table
from ..business_db import get_session
from ..schema import FutureAlarms
from ..schema import HistoryAlarms


def get_batch_insert_history_alarms_functions():
    objs = []

    class _Inner:
        def add(self, host, alarm_type, occurrence_at,
                alarm_level=None, alarm_content=None, root_cause=None,
                suggestion=None, extra_info=None
                ):
            obj = HistoryAlarms(
                host=host,
                alarm_type=alarm_type,
                alarm_level=alarm_level,
                occurrence_at=occurrence_at,
                alarm_content=alarm_content,
                root_cause=root_cause,
                suggestion=suggestion,
                extra_info=extra_info
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_history_alarm(host=None, alarm_type=None, alarm_level=None, start_occurrence_time=None,
                         end_occurrence_time=None, group: bool = False):
    with get_session() as session:
        if group:
            result = session.query(
                HistoryAlarms.host,
                HistoryAlarms.alarm_content,
                HistoryAlarms.root_cause,
                HistoryAlarms.suggestion,
                func.count(HistoryAlarms.root_cause)
            )
        else:
            result = session.query(HistoryAlarms)
        if host:
            result = result.filter(HistoryAlarms.host == host)
        if alarm_type:
            result = result.filter(HistoryAlarms.alarm_type == alarm_type)
        if alarm_level:
            result = result.filter(HistoryAlarms.alarm_level == alarm_level)
        if start_occurrence_time is not None:
            result = result.filter(HistoryAlarms.occurrence_at >= start_occurrence_time)
        if end_occurrence_time is not None:
            result = result.filter(HistoryAlarms.occurrence_at <= end_occurrence_time)
        if group:
            return result.group_by(
                HistoryAlarms.root_cause, HistoryAlarms.host,
                HistoryAlarms.alarm_content, HistoryAlarms.suggestion
            )
        return result.order_by(desc(HistoryAlarms.occurrence_at))


def count_history_alarms(host=None, alarm_type=None, alarm_level=None):
    return select_history_alarm(host, alarm_type, alarm_level).count()


def delete_timeout_history_alarms(oldest_occurrence_time):
    with get_session() as session:
        session.query(HistoryAlarms).filter(
            HistoryAlarms.occurrence_at <= oldest_occurrence_time
        ).delete()


def truncate_history_alarm():
    truncate_table(HistoryAlarms.__tablename__)


def update_history_alarm(alarm_id, alarm_status=None, recovery_time=None):
    kwargs = dict()
    if alarm_status is not None:
        kwargs.update(alarm_status=alarm_status)
    if recovery_time is not None:
        kwargs.update(recovery_at=recovery_time)
    if len(kwargs) == 0:
        return

    with get_session() as session:
        session.execute(
            update(HistoryAlarms)
                .where(HistoryAlarms.history_alarm_id == alarm_id)
                .values(**kwargs)
                .execution_options(synchronize_session="fetch")
        )


def get_batch_insert_future_alarms_functions():
    objs = []

    class _Inner:
        def add(self, host, metric_name, alarm_type,
                alarm_level=None, start_at=None,
                end_at=None, alarm_content=None, extra_info=None
                ):
            obj = FutureAlarms(
                host=host,
                metric_name=metric_name,
                alarm_type=alarm_type,
                alarm_level=alarm_level,
                start_at=start_at,
                end_at=end_at,
                alarm_content=alarm_content,
                extra_info=extra_info
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_future_alarm(metric_name=None, host=None, start_at=None, group: bool = False):
    with get_session() as session:
        if group:
            result = session.query(
                FutureAlarms.host,
                FutureAlarms.alarm_content,
                FutureAlarms.suggestion,
                func.count(FutureAlarms.alarm_content)
            )
        else:
            result = session.query(FutureAlarms)
        if metric_name:
            result = result.filter(FutureAlarms.metric_name == metric_name)
        if host:
            result = result.filter(FutureAlarms.host == host)
        if start_at is not None:
            result = result.filter(FutureAlarms.start_at >= start_at)

        if group:
            return result.group_by(FutureAlarms.alarm_content, FutureAlarms.host, FutureAlarms.suggestion)
        return result.order_by(desc(FutureAlarms.start_at))


def count_future_alarms(metric_name=None, host=None, start_at=None):
    return select_future_alarm(metric_name, host, start_at).count()


def truncate_future_alarm():
    truncate_table(FutureAlarms.__tablename__)
