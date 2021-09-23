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

agent_logger = logging.getLogger('agent')
log_dir_realpath = os.path.realpath(config.get('log', 'log_dir'))
if not os.path.exists(log_dir_realpath):
    os.makedirs(log_dir_realpath)

agent_handler = handlers.RotatingFileHandler(filename=os.path.join(log_dir_realpath, 'agent.log'),
                                             maxBytes=1024 * 1024 * 100,
                                             backupCount=5)
agent_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
agent_logger.addHandler(agent_handler)
agent_logger.setLevel(logging.INFO)
