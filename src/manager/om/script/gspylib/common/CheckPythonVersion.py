#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
import sys
import platform
import re


def checkPythonVersion():
    pythonVersion = sys.version_info[0:2]
    distName = platform.platform()
    if re.search("oe1", distName) is not None:
        if not pythonVersion == (3, 7):
            raise Exception("[GAUSS-52200] : version of python"
                            " is not correct: %s." %
                            distName + " should use Python 3.7.*")
    else:
        if not pythonVersion >= (3, 6):
            raise Exception("[GAUSS-52200] : version of python"
                            " is not correct: %s." %
                            distName + " should use Python 3.6.*")
    return True


if __name__ == '__main__':
    try:
        checkPythonVersion()
    except Exception as e:
        raise Exception(e)
