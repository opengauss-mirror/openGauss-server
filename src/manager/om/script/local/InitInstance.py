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
# Description  : InitInstance.py is a utility to init instance.
#############################################################################

import getopt
import sys
import os

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.threads.parallelTool import parallelTool

########################################################################
# Global variables define
########################################################################
g_opts = None

########################################################################
INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2


########################################################################
class CmdOptions():
    """
    """

    def __init__(self):
        """
        constructor
        """
        self.clusterUser = ""
        self.dbInitParams = []
        self.logFile = ""
        self.dws_mode = False
        self.vc_mode = False


def usage():
    """
Usage:
    python3 InitInstance.py -U user [-P "-PARAMETER VALUE" [...]] [-G
    "-PARAMETER VALUE" [...]] [-l logfile]
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:G:l:?",
                                   ["help", "dws_mode", "vc_mode"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    for (key, value) in opts:
        if (key == "-?" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_opts.clusterUser = value
        elif (key == "-P"):
            g_opts.dbInitParams.append(value)
        elif (key == "-l"):
            g_opts.logFile = os.path.realpath(value)
        elif (key == "--dws_mode"):
            g_opts.dws_mode = True
        elif (key == "--vc_mode"):
            g_opts.vc_mode = True
        Parameter.checkParaVaild(key, value)


def __checkInitdbParams(param):
    """
    function : Check parameter for initdb
        -D, --pgdata : this has been specified in configuration file
        -W, --pwprompt: this will block the script
        --pwfile: it is not safe to read password from file
        -A, --auth,--auth-local,--auth-host: They will be used with '--pwfile'
        -c, --enpasswd: this will confuse the default password in script
        with the password user specified
        -Z: this has been designated internal
        -U --username: use the user specified during install step
    input  : String
    output : Number
    """
    shortInvalidArgs = ("-D", "-W", "-C", "-A", "-Z", "-U", "-X", "-s")
    longInvalidArgs = (
    "--pgdata", "--pwprompt", "--enpasswd", "--pwfile", "--auth",
    "--auth-host", "--auth-local", "--username", "--xlogdir", "--show")
    argList = param.split()
    for arg in shortInvalidArgs:
        if (arg in argList):
            return 1

    argList = param.split("=")
    for arg in longInvalidArgs:
        if (arg in argList):
            return 1

    return 0


def checkParameter():
    """
    """
    # check if user exist and is the right user
    if (g_opts.clusterUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")
    try:
        DefaultValue.checkUser(g_opts.clusterUser, False)
    except Exception as e:
        GaussLog.exitWithError(str(e))

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                                   g_opts.clusterUser, "")

    for param in g_opts.dbInitParams:
        if (__checkInitdbParams(param.strip()) != 0):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % param)


class initDbNode(LocalBaseOM):
    '''
    classdocs
    '''

    def __init__(self, logFile, user, dwsMode=False, dbInitParams=None):
        """
        function: init instance
        input : logFile, user, clusterConf, dbInitParams
        output: NA
        """
        if dbInitParams is None:
            dbInitParams = []
        LocalBaseOM.__init__(self, logFile, user, "", dwsMode, dbInitParams)
        if self.clusterConfig == "":
            # Read config from static config file
            self.readConfigInfo()
        else:
            self.readConfigInfoByXML()
        # get user info
        self.getUserInfo()
        if user != "" and self.user != user.strip():
            self.logger.debug("User parameter : %s." % user)
            self.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50315"] % (
            self.user, self.clusterInfo.appPath))

        # init every component
        self.initComponent()

    def initNodeInst(self, vc_mode=False):
        """
        function : Init all instance on local node
        input  : NA
        output : NA
        """
        self.logger.log("Initializing instance.")

        if not vc_mode:
            components = self.etcdCons + self.cmCons + self.gtmCons\
                         + self.cnCons + self.dnCons
        else:
            # just init dn instance
            components = self.dnCons
        try:
            # config instance in paralle
            parallelTool.parallelExecute(self.initInstance, components)
        except Exception as e:
            self.logger.logExit(str(e))

        self.logger.log("Successfully init instance information.")

    def initInstance(self, component):
        """
        function: Check instance port and IP for per component
        input : NA
        output: NA
        """
        component.initInstance()


if __name__ == '__main__':
    ##########################################################################
    # init instance
    ##########################################################################
    """
    function: init instance
              1.check dbInitParams
              2.init instance
              3.save initdb parameters into initdbParamFile
    input : NA
    output: NA
    """
    try:
        # parse and check input parameters
        parseCommandLine()
        checkParameter()

        # Initialize globals parameters
        # add g_opts.vc_mode parameter :
        # indicates whether it is a virtual cluster mode
        dbInit = initDbNode(g_opts.logFile, g_opts.clusterUser,
                            g_opts.dws_mode, g_opts.dbInitParams)
        dbInit.initNodeInst(g_opts.vc_mode)

    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
