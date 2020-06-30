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
# Description  : ConfigInstance.py is a utility to config
# CN/DN/gtm/cm_agent/cm_server instance.
#############################################################################

import getopt
import sys
import os

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue, ClusterInstanceConfig
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.common.ErrorCode import ErrorCode
from gspylib.threads.parallelTool import parallelTool

#############################################################################
# Global variables
#   INSTANCE_TYPE_UNDEFINED: the signal about instance
#   MASTER_INSTANCE: the signal about instance
#   STANDBY_INSTANCE: the signal about instance
#   DUMMY_STANDBY_INSTANCE: the signal about instance
#############################################################################
INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2
CONFIG_ITEM_TYPE = "ConfigInstance"

CONFIG_PG_FILE = "pg_config"
CONFIG_GS_FILE = "gs_config"
CONFIG_ALL_FILE = "all"
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
        function: constructor
        """
        self.clusterUser = ""
        self.dataGucParams = []
        self.configType = CONFIG_ALL_FILE
        self.clusterStaticConfigFile = ""
        self.logFile = ""
        self.alarmComponent = ""
        self.gucXml = False
        self.vcMode = False
        self.dws_mode = False
        self.clusterConf = ""


def usage():
    """
Usage:
    python3 -h | -help
    python3 ConfigInstance.py -U user
        [-T config_type]
        [-P cluster_static_config]
        [-C "PARAMETER=VALUE" [...]]
        [-D "PARAMETER=VALUE" [...]]
        [-L log]
        target file: pg_config, gs_config, all
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: parseCommandLine
    input: NA
    output: NA
    """
    try:
        paraLine = sys.argv[1]
        paraLine = DefaultValue.encodeParaline(paraLine,
                                               DefaultValue.BASE_DECODE)
        paraLine = paraLine.strip()
        paraList = paraLine.split("*==SYMBOL==*")
        opts, args = getopt.getopt(paraList[1:], "U:C:D:T:P:l:hX:",
                                   ["help", "alarm=", "gucXml",
                                    "vc_mode", "dws-mode"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_opts.clusterUser = value
        elif (key == "-D"):
            g_opts.dataGucParams.append(value)
        elif (key == "-T"):
            g_opts.configType = value
        elif (key == "-P"):
            g_opts.clusterStaticConfigFile = value
        elif (key == "-l"):
            g_opts.logFile = os.path.realpath(value)
        elif (key == "--alarm"):
            g_opts.alarmComponent = value
        elif (key == "--gucXml"):
            g_opts.gucXml = True
        elif (key == "--vc_mode"):
            g_opts.vcMode = True
        elif (key == "--dws-mode"):
            g_opts.dws_mode = True
        elif key == "-X":
            g_opts.clusterConf = os.path.realpath(value)
        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: checkParameter
    input: NA
    output: NA
    """
    # check if user exist and is the right user
    if (g_opts.clusterUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")
    DefaultValue.checkUser(g_opts.clusterUser)

    if (g_opts.configType not in [CONFIG_ALL_FILE,
                                  CONFIG_GS_FILE, CONFIG_PG_FILE]):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"]
                               % 'T' + " Value: %s." % g_opts.configType)

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, g_opts.clusterUser, "")

    if (g_opts.alarmComponent == ""):
        g_opts.alarmComponent = DefaultValue.ALARM_COMPONENT_PATH


def getAlarmDict(configItemType=None, alarmComponent=None):
    """
    function: Get Alarm configuration for om_monitor
    input : configItemType, alarmComponent
    output: NA
    """
    tmpAlarmDict = {}
    if (configItemType == "ConfigInstance"):
        tmpAlarmDict["alarm_component"] = "%s" % alarmComponent
    return tmpAlarmDict


class ConfigInstance(LocalBaseOM):
    """
    Class: ConfigInstance
    """

    def __init__(self, logFile, user, clusterConf, dwsMode=False,
                 dataParams=None, confType="",
                 clusterStaticConfigFile="", alarmComponent=""):
        """
        function: configure all instance on local node
        """
        if dataParams is None:
            dataParams = []
        LocalBaseOM.__init__(self, logFile, user, clusterConf, dwsMode)
        if (self.clusterConfig == ""):
            # Read config from static config file
            self.readConfigInfo()
        else:
            self.readConfigInfoByXML()
        # get user info
        self.getUserInfo()
        if (user != "" and self.user != user.strip()):
            self.logger.debug("User parameter : %s." % user)
            self.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50315"]
                                % (self.user, self.clusterInfo.appPath))
        # get log file info
        # init every component
        self.initComponent()

        self.dataGucParams = dataParams
        self.configType = confType
        self.clusterStaticConfigFile = clusterStaticConfigFile
        self.alarmComponent = alarmComponent
        self.__dataConfig = {}

    def __checkconfigParams(self, param):
        """
        function:
            Check parameter for postgresql.conf
            port : this is calculated automatically
        input : param
        output: int
        """
        configInvalidArgs = ["port", "alarm_component"]
        # get key name and key value
        # split by '='
        keyValue = param.split("=")
        if (len(keyValue) != 2):
            return 1
        # the type like this: "key = value"
        key = keyValue[0].strip()
        value = keyValue[1].strip()
        if key in configInvalidArgs:
            return 1

        self.__dataConfig[key] = value
        return 0

    def __checkDNInstParameters(self):
        """
        function: Check parameters for instance configuration
        input : NA
        output: NA
        """
        # Checking parameters for configuration CN and DN.
        self.logger.log("Checking parameters for configuration database node.")

        for param in self.dataGucParams:
            if self.__checkconfigParams(param.strip()) != 0:
                self.logger.logExit(ErrorCode.GAUSS_500["GAUSS_50000"]
                                    % param)

    def __modifyConfig(self):
        """
        function: Modify all instances on loacl node
        input : NA
        output: NA
        """
        self.logger.log("Modifying Alarm configuration.")
        tmpAlarmDict = getAlarmDict(self.configType, self.alarmComponent)
        # init alarmItem.conf file
        configFile = "%s/bin/alarmItem.conf" % self.clusterInfo.appPath
        ClusterInstanceConfig.setConfigItem(
            DefaultValue.INSTANCE_ROLE_CMAGENT, "", configFile, tmpAlarmDict)

        componentList = self.dnCons
        if len(componentList) == 0:
            return
        try:
            # config instance in paralle
            parallelTool.parallelExecute(self.configInst, componentList)
        except Exception as e:
            self.logger.logExit(str(e))

    def configInst(self, dbCon):
        """
        function: Config the instance
        input : dbCon
        output: NA
        """
        if dbCon.instInfo.instanceRole == DefaultValue.INSTANCE_ROLE_DATANODE:
            # modifying database node configuration.
            self.logger.log("Modifying database node configuration.")
            peerInsts = self.clusterInfo.getPeerInstance(dbCon.instInfo)
            azNames = self.clusterInfo.getazNames()
            allConfig = {}
            allConfig.update(self.__dataConfig)
            dbCon.configInstance(self.user, allConfig,
                                 peerInsts, CONFIG_ITEM_TYPE,
                                 self.alarmComponent, azNames,
                                 g_opts.gucXml, self.clusterInfo)

    def modifyInstance(self):
        """
        Class: modifyInstance
        """
        self.__checkDNInstParameters()
        # modify all instances on loacl node
        if self.configType in [CONFIG_PG_FILE, CONFIG_ALL_FILE]:
            self.__modifyConfig()


if __name__ == '__main__':
    ##########################################################################
    # config instance
    ##########################################################################
    """
    function: config instance
              1.check dbInitParams
              2.modify instance 
              3.genarate cert files
    input : NA
    output: NA
    """
    try:
        # parse and check input parameters
        parseCommandLine()
        checkParameter()

        # modify Instance
        configer = ConfigInstance(g_opts.logFile, g_opts.clusterUser,
                                  g_opts.clusterConf,
                                  False,
                                  g_opts.dataGucParams, g_opts.configType,
                                  g_opts.clusterStaticConfigFile,
                                  g_opts.alarmComponent)
        configer.modifyInstance()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
