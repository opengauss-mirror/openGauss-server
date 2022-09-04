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
"""This table mainly sets some parameters about the detection process,
 for instance, thresholds and metrics to be checked. """

from .. import DynamicConfig


class DetectionParams(DynamicConfig):
    __tablename__ = "detection_params"

    __default__ = {
        'high_ac_threshold': 0.1,
        'min_seasonal_freq': 2,
        'disk_usage_threshold': 0.8,
        'disk_usage_max_coef': 2.5e-08,  # window: 5 minutes
        'mem_usage_threshold': 0.8,
        'mem_usage_max_coef': 8e-08,  # window: 5 minutes
        'cpu_usage_threshold': 0.8,
        'cpu_high_usage_percent': 0.8,
        'tps_threshold': 2000,
        'qps_max_coef': 8e-03,  # window: 5 minutes
        'connection_max_coef': 4e-04,  # window: 5 minutes
        'p80_threshold': 260,
        'io_capacity_threshold': 25,
        'io_delay_threshold': 50,
        'io_wait_threshold': 0.1,
        'load_average_threshold': 0.6,
        'iops_threshold': 2000,
        'handler_occupation_threshold': 0.7,
        'disk_ioutils_threshold': 0.3,
        'connection_rate_threshold': 0.1,
        'connection_usage_threshold': 0.8,
        'package_drop_rate_threshold': 0.01,
        'package_error_rate_threshold': 0.01,
        'bgwriter_rate_threshold': 0.1,
        'replication_write_diff_threshold': 100000,
        'replication_sent_diff_threshold': 100000,
        'replication_replay_diff_threshold': 1000000,
        'thread_occupy_rate_threshold': 0.95,
        'idle_session_occupy_rate_threshold': 0.3,
        'double_write_file_wait_threshold': 100,
        'data_file_wait_threshold': 100000,
        'os_cpu_usage_low': 0,
        'os_cpu_usage_high': 0.8,
        'os_cpu_usage_percent': 0.8,
        'os_mem_usage_low': 0,
        'os_mem_usage_high': 0.8,
        'os_mem_usage_percent': 0.8,
        'os_disk_usage_low': 0,
        'os_disk_usage_high': 0.8,
        'os_disk_usage_percent': 0,
        'io_write_bytes_low': 0,
        'io_write_bytes_high': 2,
        'io_write_bytes_percent': 0,
        'pg_replication_replay_diff_low': 0,
        'pg_replication_replay_diff_high': 70,
        'pg_replication_replay_diff_percent': 0,
        'gaussdb_qps_by_instance_low': 0,
        'gaussdb_qps_by_instance_high': 100,
        'gaussdb_qps_by_instance_percent': 0,
    }
