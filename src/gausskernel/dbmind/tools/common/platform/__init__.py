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

_PLATFORM = os.sys.platform
LINUX = _PLATFORM == 'linux'
WIN32 = _PLATFORM == 'win32'

# declaration
if WIN32:
    from ._win32 import win32_get_process_cwd
    from ._win32 import win32_get_process_cmdline
    from ._win32 import win32_get_process_path
    from ._win32 import win32_is_process_running

