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
from sqlalchemy import Column, Integer, String, Numeric

from .. import Base


class IndexRecommendation(Base):
    __tablename__ = "tb_index_recommendation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(String(24), nullable=False)
    db_name = Column(String(64), nullable=False)
    schema_name = Column(String(32), nullable=False)
    tb_name = Column(String(64), nullable=False)
    columns = Column(String(256))
    optimized = Column(String(32))

    # 1:recommend, 2: redundancy, 3: invalid
    # the corresponding database field: index_type
    index_type = Column(Integer)
    stmt_count = Column(Integer)

    select_ratio = Column(Numeric(10, 6))
    insert_ratio = Column(Numeric(10, 6))
    update_ratio = Column(Numeric(10, 6))
    delete_ratio = Column(Numeric(10, 6))
    index_stmt = Column(String(512), nullable=False)
