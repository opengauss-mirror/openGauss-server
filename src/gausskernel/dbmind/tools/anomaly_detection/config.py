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

from configparser import ConfigParser

from global_vars import CONFIG_PATH

_config = ConfigParser()
_config.read(CONFIG_PATH)

read = _config.read
get = _config.get
getint = _config.getint
getfloat = _config.getfloat
getboolean = _config.getboolean

has_option = _config.has_option
has_section = _config.has_section
