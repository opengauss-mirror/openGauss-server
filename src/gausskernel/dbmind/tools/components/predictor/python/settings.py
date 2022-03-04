"""
 openGauss is licensed under Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:

 http://license.coscl.org.cn/MulanPSL2

 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 See the Mulan PSL v2 for more details.
 
 Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 Description: The settings for AiEngine.
"""

import os

# Flask settings
DEFAULT_FLASK_SERVER_HOST = '127.0.0.1'
DEFAULT_FLASK_SERVER_PORT = '5000'
DEFAULT_FLASK_DEBUG = '0'  # Do not use debug mode in production

# Path settings
PATH_UPLOAD = 'uploads/'
PATH_MODELS = 'saved_models/'
PATH_LOG = 'log/'
PATH_ENGINE_LOG = 'e_log/model_logs'

# Path for certifications
PATH_SSL = "path_to_CA"
PATH_CA = PATH_SSL + '/demoCA/cacert.pem'
PATH_SERVER_KEY = PATH_SSL + '/server.key'
PATH_SERVER_CRT = PATH_SSL + '/server.crt'

# GPU configuration set as '-1' if no gpu is available, default two gpus
GPU_CONFIG = '0,1'

# Path for logs
base_path = os.path.dirname(__file__)
PATH_MODELS_INFO = os.path.realpath(os.path.join(base_path, PATH_MODELS))
