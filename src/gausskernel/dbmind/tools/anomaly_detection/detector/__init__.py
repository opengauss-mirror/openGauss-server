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

import logging
import os
from logging import handlers

import config

log_dir_realpath = os.path.realpath(config.get('log', 'log_dir'))
if not os.path.exists(log_dir_realpath):
    os.makedirs(log_dir_realpath)

detector_logger = logging.getLogger('detector')
detector_handler = handlers.RotatingFileHandler(filename=os.path.join(log_dir_realpath, 'detector.log'),
                                                maxBytes=1024 * 1024 * 100,
                                                backupCount=5)
detector_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
detector_logger.addHandler(detector_handler)
detector_logger.setLevel(logging.INFO)

service_logger = logging.getLogger('service')
service_handler = handlers.RotatingFileHandler(filename=os.path.join(log_dir_realpath, 'service.log'),
                                               maxBytes=1024 * 1024 * 100,
                                               backupCount=5)
service_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
service_logger.addHandler(service_handler)
service_logger.setLevel(logging.INFO)

abnormal_logger = logging.getLogger('abnormal')
abnormal_handler = handlers.RotatingFileHandler(filename=os.path.join(log_dir_realpath, 'abnormal.log'),
                                                maxBytes=1024 * 1024 * 100,
                                                backupCount=5)
abnormal_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
abnormal_logger.addHandler(abnormal_handler)
abnormal_logger.setLevel(logging.INFO)

sql_rca_logger = logging.getLogger('sql_rca')
sql_rca_handler = handlers.RotatingFileHandler(filename=os.path.join(log_dir_realpath, 'sql_rca.log'),
                                               maxBytes=1024 * 1024 * 100,
                                               backupCount=5)
sql_rca_handler.setFormatter(logging.Formatter("%(message)s"))
sql_rca_logger.addHandler(sql_rca_handler)
sql_rca_logger.setLevel(logging.INFO)
