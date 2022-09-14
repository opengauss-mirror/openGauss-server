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

from sqlalchemy import Column, String, Integer, BigInteger, CHAR, JSON, Index

from .. import Base


class FutureAlarms(Base):
    __tablename__ = "tb_future_alarms"

    future_alarm_id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(CHAR(24), nullable=False)
    metric_name = Column(String(64), nullable=False)
    alarm_type = Column(String(16), nullable=False)
    alarm_level = Column(String(16))
    start_at = Column(BigInteger)  # unix timestamp
    end_at = Column(BigInteger)
    alarm_content = Column(String(1024))
    suggestion = Column(String(1024))
    extra_info = Column(JSON(none_as_null=True))

    idx_future_alarms = Index("idx_future_alarms", metric_name, host, start_at)
