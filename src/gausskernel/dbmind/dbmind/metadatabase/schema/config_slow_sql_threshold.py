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
from .. import DynamicConfig


class SlowSQLThreshold(DynamicConfig):
    __tablename__ = "slow_sql_threshold"

    __default__ = {
        'tuple_number_threshold': 5000,
        'table_total_size_threshold': 2048,
        'fetch_tuples_threshold': 10000,
        'returned_rows_threshold': 1000,
        'updated_tuples_threshold': 1000,
        'deleted_tuples_threshold': 1000,
        'inserted_tuples_threshold': 1000,
        'hit_rate_threshold': 0.95,
        'dead_rate_threshold': 0.2,
        'index_number_threshold': 3,
        'index_number_rate_threshold': 0.6,
        'max_elapsed_time': 60,
        'update_statistics_threshold': 60,
        'nestloop_rows_threshold': 10000,
        'hashjoin_rows_threshold': 10000,
        'groupagg_rows_threshold': 10000,
        'cost_rate_threshold': 0.4,
        'plan_time_occupy_rate_threshold': 0.3,
        'used_index_tuples_rate_threshold': 0.2
    }
