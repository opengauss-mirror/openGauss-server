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
from sqlalchemy import Column, String, BigInteger, Integer, Float, Index, TEXT, CHAR, Boolean

from .. import Base


class SlowQueries(Base):
    __tablename__ = "tb_slow_queries"

    slow_query_id = Column(
        BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True
    )
    address = Column(CHAR(24), nullable=False)
    schema_name = Column(String(64), nullable=False)
    db_name = Column(String(64), nullable=False)
    query = Column(TEXT, nullable=False)
    # (s)elect, (d)elete, (i)nsert, (u)pdate, null(others, such as DDL)
    query_type = Column(CHAR(1), nullable=True)
    involving_systable = Column(Boolean, nullable=False)
    template_id = Column(BigInteger)
    # Use two hash codes to avoid hash conflicts.
    hashcode1 = Column(BigInteger, nullable=False)
    hashcode2 = Column(BigInteger, nullable=True)
    insert_at = Column(BigInteger, nullable=False)
    hit_rate = Column(Float)
    fetch_rate = Column(Float)
    cpu_time = Column(Float)
    data_io_time = Column(Float)
    plan_time = Column(Float)
    parse_time = Column(Float)
    db_time = Column(Float)
    root_cause = Column(TEXT)
    suggestion = Column(TEXT)

    idx_slow_query_hashcode = Index("idx_slow_query_hashcode", hashcode1, hashcode2)

