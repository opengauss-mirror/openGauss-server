"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

import os
import time

CURRENT_DIRNAME = os.path.dirname(os.path.abspath(__file__))
PROJECT_NAME = os.path.basename(CURRENT_DIRNAME)
CONFIG_PATH = os.path.join(CURRENT_DIRNAME, 'a-detection.conf')
METRIC_CONFIG_PATH = os.path.join(CURRENT_DIRNAME, 'task/metric_task.conf')
BIN_PATH = os.path.join(CURRENT_DIRNAME, 'bin')
TABLE_INFO_PATH = os.path.join(CURRENT_DIRNAME, 'table.json')
TASK_PATH = os.path.join(CURRENT_DIRNAME, 'task')
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

APP_START_TIME = int(time.time())
SLOW_START_TIME = APP_START_TIME
DETECTOR_START_TIME = APP_START_TIME
