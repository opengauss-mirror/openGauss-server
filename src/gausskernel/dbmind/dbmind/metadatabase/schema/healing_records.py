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

from sqlalchemy import Column, String, Integer, BigInteger, CHAR, Index, Boolean

from .. import Base


class HealingRecords(Base):
    __tablename__ = "tb_healing_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(CHAR(64), nullable=False)
    trigger_events = Column(String(1024), nullable=True)
    trigger_root_causes = Column(String(1024), nullable=True)
    action = Column(String(64), nullable=True)
    called_method = Column(String(128), nullable=False)
    success = Column(Boolean, nullable=False)
    detail = Column(String(256), nullable=True)
    occurrence_at = Column(BigInteger, nullable=False)  # unix timestamp

    idx_healing_records = Index("idx_healing_records", occurrence_at, called_method)
