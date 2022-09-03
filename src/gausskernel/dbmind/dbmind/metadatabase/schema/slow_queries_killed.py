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
from sqlalchemy import Column, String, TEXT, Integer, Float, Boolean, BigInteger

from .. import Base


class SlowQueriesKilled(Base):
    __tablename__ = "tb_killed_slow_queries"

    killed_query_id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(64), nullable=False)
    query = Column(TEXT, nullable=False)
    killed = Column(Boolean, nullable=False)
    username = Column(String(64), nullable=False)
    elapsed_time = Column(Float, nullable=False)
    killed_time = Column(BigInteger, nullable=False)
