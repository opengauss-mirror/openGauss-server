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
from sqlalchemy import Column, String, Float

from .. import DynamicConfig

_default = {
    'tuple_number_limit': 5000,
    'fetch_tuples_limit': 10000,
    'fetch_rate_limit': 0.3,
    'returned_rows_limit': 1000,
    'returned_rate_limit': 0.3,
    'updated_tuples_limit': 1000,
    'updated_rate_limit': 1000,
    'deleted_tuples_limit': 1000,
    'deleted_rate_limit': 0.3,
    'inserted_tuples_limit': 1000,
    'inserted_rate_limit': 0.3,
    'hit_rate_limit': 0.95,
    'dead_rate_limit': 0.2,
    'index_number_limit': 3,
    'load_average_rate_limit': 0.6,
    'cpu_usage_limit': 0.5,
    'iops_limit': 0.5,
    'ioutils_limit': 0.5,
    'iowait_limit': 0.05,
    'tps_limit': 2000,
    'iocapacity_limit': 50
}


class SlowSQLThreshold(DynamicConfig):
    __tablename__ = "slow_sql_threshold"

    name = Column(String, primary_key=True)
    value = Column(Float, nullable=False)

    @staticmethod
    def default_values():
        rows = []
        for name, value in _default.items():
            rows.append(SlowSQLThreshold(name=name, value=value))
        return rows
