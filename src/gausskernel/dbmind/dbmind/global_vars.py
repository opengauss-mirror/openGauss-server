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
"""Using this global_vars must import as the following:

    >>> from dbmind import global_vars

Notice: The variables in the following should be used across files.
Otherwise, if a variable is not used across files,
it should be placed where the nearest used position is.
"""
import multiprocessing

configs = None
dynamic_configs = None
metric_map = None
metric_value_range_map = None
must_filter_labels = None
worker = None
confpath = None
backend_timed_task = []
is_dry_run_mode = False
agent_rpc_client = None
self_driving_records = None
executor_lock = multiprocessing.Lock()
