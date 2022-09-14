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
from sqlalchemy import Column, Integer, String, BigInteger

from .. import Base


class IndexRecommendationStmtDetails(Base):
    __tablename__ = 'tb_index_recommendation_stmt_details'

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(32), nullable=False)
    index_id = Column(BigInteger)  # ForeignKey('tb_index_recommendation.id', ondelete='CASCADE')
    template_id = Column(BigInteger)  # ForeignKey('tb_index_recommendation_stmt_templates.id', ondelete='CASCADE')
    stmt = Column(String(4096))
    optimized = Column(String(16))
    correlation_type = Column(Integer)
    stmt_count = Column(Integer)
