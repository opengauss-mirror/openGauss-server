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
from sqlalchemy import Column, String, Integer, BigInteger, CHAR, Index, Numeric

from .. import Base


class ForecastingMetrics(Base):
    __tablename__ = "tb_forecasting_metrics"

    rowid = Column(Integer, primary_key=True, autoincrement=True)
    metric_name = Column(String(64), nullable=False)
    host = Column(CHAR(24), nullable=False)
    metric_value = Column(Numeric(16, 2), nullable=False)
    metric_time = Column(BigInteger, nullable=False)
    labels = Column(String, nullable=True)

    idx_forecasting_metrics = Index("idx_forecasting_metrics", metric_name, host, metric_time)
