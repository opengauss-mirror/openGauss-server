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
    metric_name = Column(String(32), nullable=False)
    metric_type = Column(String(8))
    host_ip = Column(CHAR(24), nullable=False)
    node_id = Column(BigInteger)
    metric_value = Column(Numeric(16, 2), nullable=False)
    metric_time = Column(BigInteger, nullable=False)

    idx_forecasting_metrics = Index("idx_forecasting_metrics", metric_name, host_ip, metric_time)
