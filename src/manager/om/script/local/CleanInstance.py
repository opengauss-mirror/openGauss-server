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
# Description  : CleanInstance.py is a utility to clean Gauss MPP
#               Database instance.
#############################################################################
import getopt
import os
import time
import sys

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue, ClusterInstanceConfig
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.threads.parallelTool import parallelTool
from gspylib.os.gsOSlib import g_OSlib

########################################################################
INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2
TYPE_DATADIR = "data-dir"
TYPE_LOCKFILE = "lock-file"
TIME_INIERVAL = 3

########################################################################
# Global variables define
########################################################################
g_opts = None


########################################################################
class CmdOptions():
    """
    class: cmdOptions
    """

    def __init__(self):
        """
        function: Constructor
        input : NA
        output: NA
        """
        self.clusterInfo = None
        self.dbNodeInfo = None
        self.logger = None

        self.cleanType = []
        self.Instancedirs = []
        self.tblspcdirs = []
        self.user = ""
        self.group = ""
        self.clusterConfig = ""
        self.failedDir = ""
        self.nodedirCount = 0
        self.inputDir = False
        self.logFile = ""


##############################################################################
# Help context.
##############################################################################
def usage():
    """
CleanInstance.py is a utility to clean Gauss MPP Database instance.

Usage:
  python3 CleanInstance.py --help
  python3 CleanInstance.py -U user
   [-t cleanType...]
   [-D datadir...]
   [-l logfile]
   [-X clusterConfig]

Common options:
  --help    show this help, then exit
  -U        the user of Gauss MPP Database
  -t        the content to be cleaned, can be data directory, lock-file.
  -D        the directory of instance to be clean.
  -l        the log file path
  -X        the path of XML configuration file
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: check input parameters
    input : NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:D:l:t:X:", ["help"])
    except getopt.GetoptError as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    for key, value in opts:
        if (key == "-U"):
            g_opts.user = value
        elif (key == "-D"):
            g_opts.inputDir = True
            g_opts.Instancedirs.append(os.path.normpath(value))
        elif (key == "-t"):
            g_opts.cleanType.append(value)
        elif (key == "-l"):
            g_opts.logFile = value
        elif (key == "-X"):
            g_opts.clusterConfig = value
        elif (key == "--help"):
            usage()
            sys.exit(0)
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % key)
        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    class: check parameter
    """
    # check if user exist and is the right user
    if (g_opts.user == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")
    try:
        DefaultValue.checkUser(g_opts.user, False)
    except Exception as e:
        GaussLog.exitWithError(str(e))

    if (os.getuid() == 0 and g_opts.clusterConfig is None):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'X' + ".")

    if (len(g_opts.cleanType) == 0):
        g_opts.cleanType = [TYPE_DATADIR, TYPE_LOCKFILE]

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, g_opts.user, "", "")


class CleanInstance(LocalBaseOM):
    """
    class: cleanInstance
    """

    def __init__(self, logFile, user, clusterConf, dwsMode=False):
        """
        function: Constructor
        input : logFile, user, clusterConf, dwsMode
        output: NA
        """
        LocalBaseOM.__init__(self, logFile, user, clusterConf, dwsMode)
        if (self.clusterConfig == ""):
            # Read config from static config file
            self.readConfigInfo()
        elif self.clusterConfig.endswith("json"):
            self.readConfigInfoByJson()
        else:
            self.readConfigInfoByXML()
        # get user info
        self.getUserInfo()
        if (user != "" and self.user != user.strip()):
            self.logger.debug("User parameter : %s." % user)
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50315"]
                            % (self.user, self.clusterInfo.appPath))
        # init every component
        self.initComponent()

    ##########################################################################
    # This is the main clean instance flow.
    ##########################################################################
    def cleanInstance(self):
        """
        function: Clean node instances.
                  1.get the data dirs, tablespaces, soketfiles
                  2.use theard delete the dirs or files
        input : NA
        output: NA
        """
        self.logger.log("Cleaning instance.")
        compentsList = []

        for compent in self.dnCons:
            if ((g_opts.inputDir) and
                    (compent.instInfo.datadir not in g_opts.Instancedirs) and
                    (compent.instInfo.ssdDir not in g_opts.Instancedirs)):
                continue
            peerInsts = self.clusterInfo.getPeerInstance(compent.instInfo)
            nodename = ClusterInstanceConfig. \
                setReplConninfoForSinglePrimaryMultiStandbyCluster(
                    compent.instInfo, peerInsts, self.clusterInfo)[1]
            comList = []
            comList.append(compent)
            comList.append(nodename)
            compentsList.append(comList)

        if (len(compentsList) != 0):
            try:
                self.logger.debug("Deleting instances.")
                parallelTool.parallelExecute(self.uninstallCompent,
                                             compentsList)
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully cleaned instances.")

        self.logger.log("Successfully cleaned instance information.")

    def uninstallCompent(self, compentEle=None):
        """
        function: uninstall compent
        input: NA
        output: NA
        """
        if compentEle is None:
            compentEle = []
        if len(compentEle) == 1:
            compentEle[0].uninstall()
        if len(compentEle) == 2:
            compentEle[0].uninstall(compentEle[1])

    def killProcess(self):
        """
        function: kill process for cleaning instance data.
        input : NA
        output: NA
        """
        pidList = g_OSlib.getProcess("gs_initdb")
        if len(pidList) == 0:
            return
        self.logger.debug("Initdb process exists.")
        g_OSlib.killProcessByProcName("gs_initdb")
        while (True):
            pidList = g_OSlib.getProcess("gs_initdb")
            if len(pidList) != 0:
                time.sleep(TIME_INIERVAL)
            else:
                self.logger.debug("Initdb process is deleted.")
                break


if __name__ == '__main__':
    ##########################################################################
    # clean instance
    ##########################################################################
    """
    function: Do clean instance
              1.get the clusterinfo and daNodeInfo
              2.check the user and group
              3.clean the instance
    input : NA
    output: NA
    """
    try:
        # parse and check input parameters
        parseCommandLine()
        checkParameter()

        # Initialize globals parameters
        cleanInst = CleanInstance(g_opts.logFile, g_opts.user,
                                  g_opts.clusterConfig)
        cleanInst.cleanInstance()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
