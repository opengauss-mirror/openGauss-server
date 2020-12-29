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
from configparser import ConfigParser

from main import config_path
from utils import detection_logger

config = ConfigParser()
config.read(config_path)
log_dir_realpath = os.path.realpath(config.get('log', 'log_dir'))
if not os.path.exists(log_dir_realpath):
    os.makedirs(log_dir_realpath)
logger = detection_logger(level='INFO',
                          log_name='monitor',
                          log_path=os.path.join(log_dir_realpath, 'monitor.log'))
