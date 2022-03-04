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


class SlowQueries(Base):
    __tablename__ = "tb_slow_queries"

    slow_query_id = Column(Integer, primary_key=True, autoincrement=True)
    schema_name = Column(String(64), nullable=False)
    db_name = Column(String(64), nullable=False)
    query = Column(String(1024), nullable=False)
    template_id = Column(BigInteger)
    start_at = Column(BigInteger, nullable=False)
    duration_time = Column(Float, nullable=False)
    hit_rate = Column(Float)
    fetch_rate = Column(Float)
    cpu_time = Column(Float)
    data_io_time = Column(Float)
    root_cause = Column(String(1024))
    suggestion = Column(String(1024))

    idx_slow_queries = Index("idx_slow_queries", duration_time, start_at)

