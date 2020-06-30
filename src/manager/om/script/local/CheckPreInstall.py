#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
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
# Description  : CheckPreInstall.py is a utility to check whether the
# PreInstall has been done or not.
#############################################################################
import subprocess
import getopt
import sys
import os
import pwd

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode

PREINSTALL_FLAG = "1"
INSTALL_FLAG = "2"


def usage():
    """
Usage:
    python3 CheckPreInstall.py -h|--help
    python3 CheckPreInstall.py -U user
    """
    print(usage.__doc__)


def main():
    """
    function: main function:
              1.parse parameter
              2.check $GAUSS_ENV
    input : NA
    output: NA
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "U:h:t:", ["help"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    DBUser = ""
    checkInstall = "preinstall"
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            DBUser = value
        elif (key == "-t"):
            checkInstall = value
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % key)
        # check para vaild
        Parameter.checkParaVaild(key, value)

    # check user
    if (DBUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")

    try:
        execUser = pwd.getpwuid(os.getuid()).pw_name
        if (execUser != DBUser):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"] % "U")
        # Check if user exists and if is the right user
        DefaultValue.checkUser(DBUser, False)
    except Exception as e:
        GaussLog.exitWithError(str(e))
    # check if have done preinstall for this user
    cmd = "echo $GAUSS_ENV 2>/dev/null"
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_518["GAUSS_51802"] % "GAUSS_ENV")
    if checkInstall == "preinstall":
        if (
                output.strip() == PREINSTALL_FLAG or output.strip() ==
                INSTALL_FLAG):
            GaussLog.printMessage("Successfully checked GAUSS_ENV.")
            sys.exit(0)
        else:
            GaussLog.exitWithError(
                ErrorCode.GAUSS_518["GAUSS_51805"] % "GAUSS_ENV")
    elif checkInstall == "install" and output.strip() == INSTALL_FLAG:
        GaussLog.exitWithError(ErrorCode.GAUSS_518["GAUSS_51806"])


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        GaussLog.exitWithError(str(e))
