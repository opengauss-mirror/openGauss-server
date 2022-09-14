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
from sqlalchemy import Column, Integer, String

from .. import Base


class ExistingIndexes(Base):
    __tablename__ = "tb_existing_indexes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(String(24), nullable=False)
    db_name = Column(String(32), nullable=False)
    tb_name = Column(String(64), nullable=False)
    columns = Column(String(256))
    index_stmt = Column(String(512), nullable=False)
