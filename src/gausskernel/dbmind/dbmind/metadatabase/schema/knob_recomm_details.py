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
from sqlalchemy import Column, Integer, String, Numeric, Boolean

from .. import Base


class KnobRecommendationDetails(Base):
    __tablename__ = "tb_knob_recommendation_details"

    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(String(24), nullable=False)
    name = Column(String(128), nullable=False)
    current = Column(Numeric(), nullable=False)
    recommend = Column(Numeric(), nullable=False)
    min = Column(Numeric(), nullable=False)
    max = Column(Numeric(), nullable=False)
    restart = Column(Boolean(), nullable=False)
