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
from sqlalchemy import Column, String, Integer, BigInteger, Float, Index

from .. import Base


class StatisticalMetric(Base):
    __tablename__ = "tb_stat_one_month"
    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_name = Column(String(64), nullable=False)
    host = Column(String(24), nullable=False)
    avg = Column(Float)
    min = Column(Float)
    max = Column(Float)
    the_95_quantile = Column(Float)
    date = Column(BigInteger, nullable=False)
    idx_stat = Index("idx_stat_metric_name", metric_name)
