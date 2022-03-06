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
"""This IV table is just for AES encryption."""
from sqlalchemy import Column, String

from .. import DynamicConfig

_default = {
    # Nothing before inserting.
}


class IV_Table(DynamicConfig):
    __tablename__ = "iv_table"

    name = Column(String, primary_key=True)
    value = Column(String, nullable=False)

    @staticmethod
    def default_values():
        # return empty list rather than NoneType.
        # Otherwise, raise TypeError: 'NoneType' object is not iterable.
        return []


