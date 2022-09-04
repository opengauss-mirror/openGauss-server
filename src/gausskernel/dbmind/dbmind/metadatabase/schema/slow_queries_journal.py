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
from sqlalchemy import Column, BigInteger, Integer, Float, Index

from .. import Base


class SlowQueriesJournal(Base):
    __tablename__ = "tb_slow_queries_journal"

    journal_id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    # Refer to tb_slow_queries.slow_query_id.
    slow_query_id = Column(BigInteger().with_variant(Integer, 'sqlite'), nullable=False)
    start_at = Column(BigInteger, nullable=False)
    round_start_at = Column(BigInteger, nullable=False)
    duration_time = Column(Float, nullable=False)

    idx_faked_foreign_key = Index("idx_slow_queries_journal_slow_query_id", slow_query_id)
