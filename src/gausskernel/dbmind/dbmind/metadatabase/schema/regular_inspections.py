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
from sqlalchemy import Column, String, Integer, BigInteger, Index, TEXT

from .. import Base


class RegularInspection(Base):
    __tablename__ = "tb_regular_inspections"
    id = Column(Integer, primary_key=True, autoincrement=True)
    inspection_type = Column(String(64), nullable=False)
    report = Column(TEXT)
    conclusion = Column(TEXT)
    start = Column(BigInteger, nullable=False)
    end = Column(BigInteger, nullable=False)
    idx_knob_inspection = Index("idx_knob_inspection", inspection_type)
