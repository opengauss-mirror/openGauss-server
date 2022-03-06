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
from sqlalchemy import Column, String

from .. import DynamicConfig

_default = {
    'cipher_s1': '',
    'cipher_s2': ''
}


class DBMindConfig(DynamicConfig):
    __tablename__ = "dbmind_config"

    name = Column(String, primary_key=True)
    value = Column(String, nullable=False)

    @staticmethod
    def default_values():
        rows = []
        for name, value in _default.items():
            rows.append(DBMindConfig(name=name, value=value))
        return rows
