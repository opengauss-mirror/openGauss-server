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
# Description  : CreatePath.py is a utility to create new path.
#############################################################################
import getopt
import sys
import os
import subprocess
import pwd
import grp

sys.path.append(sys.path[0] + "/../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue

g_user = ""
g_newPath = ""


def exitWithRetCode(retCode, msg=""):
    """
    exit with retcode message
    """
    if (msg != ""):
        print(msg)
    sys.exit(retCode)


def usage():
    """
Usage:
  python3 CreatePath.py -U user -P newpath
Common options:
  -U                               the user of old cluster
  -P                               the new path need to be created
  --help                           show this help, then exit
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: Check parameter from command line
    input : NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:h", ["help"])
    except Exception as e:
        usage()
        exitWithRetCode(1, ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        exitWithRetCode(1, ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    global g_user
    global g_newPath

    for (key, value) in opts:
        if (key == "--help" or key == "-h"):
            usage()
            exitWithRetCode(0)
        elif (key == "-U"):
            g_user = value
        elif (key == "-P"):
            g_newPath = value
        else:
            exitWithRetCode(1, ErrorCode.GAUSS_500["GAUSS_50000"] % key)
        Parameter.checkParaVaild(key, value)

    if (g_user == ""):
        exitWithRetCode(1, ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")
    if (g_newPath == ""):
        exitWithRetCode(1, ErrorCode.GAUSS_500["GAUSS_50001"] % 'P' + ".")
    if (not os.path.isabs(g_newPath)):
        exitWithRetCode(1, ErrorCode.GAUSS_502["GAUSS_50213"] % g_newPath)
    g_newPath = os.path.normpath(g_newPath)


def getTopPathNotExist(topDirPath):
    """
    function: find the top path to be created
    output: tmpDir
    """
    tmpDir = topDirPath
    while True:
        # find the top path to be created
        (tmpDir, topDirName) = os.path.split(tmpDir)
        if (os.path.exists(tmpDir) or topDirName == ""):
            tmpDir = os.path.join(tmpDir, topDirName)
            break
    return tmpDir


def createPathUnderRoot(newPath, user):
    """
    create path using root user
    this function only can be called by root, and user should be exist
    input : newPath, user
    output: NA
    """
    # get group information
    try:
        DefaultValue.getUserId(user)
    except Exception as e:
        exitWithRetCode(1, str(e))
    groupInfo = grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name

    # check and create new path
    ownerPath = newPath
    newPathExistAlready = True
    if (not os.path.exists(ownerPath)):
        newPathExistAlready = False
        ownerPath = getTopPathNotExist(ownerPath)
    # create newPath
    cmd = "(if [ ! -d '%s' ]; then mkdir -p '%s' -m %s;fi)" \
          % (newPath, newPath, DefaultValue.KEY_DIRECTORY_MODE)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        msg = "Cmd:%s\noutput:%s" % (cmd, output)
        exitWithRetCode(1, msg)
    # give permissions to the directory ownerPath
    if not newPathExistAlready:
        cmd = "chown -R %s:%s '%s' && chmod -R %s '%s'" \
              % (user, groupInfo, ownerPath,
                 DefaultValue.KEY_DIRECTORY_MODE, ownerPath)
    else:
        cmd = "chown %s:%s '%s' && chmod %s '%s'" \
              % (user, groupInfo, ownerPath,
                 DefaultValue.KEY_DIRECTORY_MODE, ownerPath)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        msg = "Cmd:%s\noutput:%s" % (cmd, output)
        exitWithRetCode(1, msg)

    # check enter permission
    cmd = "su - %s -c 'cd %s'" % (user, newPath)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        msg = "Cmd:%s\noutput:%s" % (cmd, output)
        exitWithRetCode(1, msg)

    # create new path succeed, return 0
    exitWithRetCode(0, "Successfully created new path.")


if __name__ == '__main__':
    """
    main function
    """
    # check precondition
    if (os.getuid() != 0):
        exitWithRetCode(1, ErrorCode.GAUSS_501["GAUSS_50104"])
    try:
        parseCommandLine()
        createPathUnderRoot(g_newPath, g_user)
    except Exception as e:
        exitWithRetCode(1, str(e))
