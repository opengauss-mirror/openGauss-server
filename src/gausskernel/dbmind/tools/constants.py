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
import os

__version__ = '1.0.0'
__description__ = 'openGauss DBMind: An autonomous platform for openGauss'

DBMIND_PATH = os.path.dirname(os.path.realpath(__file__))
MISC_PATH = os.path.join(DBMIND_PATH, 'misc')

CONFILE_NAME = 'dbmind.conf'  # the name of configuration file
PIDFILE_NAME = 'dbmind.pid'
LOGFILE_NAME = 'dbmind.log'
METRIC_MAP_CONFIG = 'metric_map.conf'
MUST_FILTER_LABEL_CONFIG = 'filter_label.conf'
METRIC_VALUE_RANGE_CONFIG = "metric_value_range.conf"
DYNAMIC_CONFIG = 'dynamic_config.db'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

DBMIND_CORE_CONTROLLER = 'dbmind.controllers.dbmind_core'

# The following list shows tasks that may be dispatched in the backend.
SLOW_QUERY_DIAGNOSIS_NAME = 'slow_query_diagnosis'
FORECAST_NAME = 'forecast'
ANOMALY_DETECTION_NAME = 'anomaly_detection'
ALARM_LOG_DIAGNOSIS_NAME = 'alarm_log_diagnosis'
TIMED_TASK_NAMES = (
    SLOW_QUERY_DIAGNOSIS_NAME, FORECAST_NAME
)
