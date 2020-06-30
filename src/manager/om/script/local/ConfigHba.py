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
# Description  : ConfigHba.py is a utility to config Hba instance.
#############################################################################

import getopt
import os
import sys

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.threads.parallelTool import parallelTool

########################################################################
# Global variables define
########################################################################
g_opts = None


########################################################################
class CmdOptions():
    """
    """

    def __init__(self):
        """
        """
        self.clusterUser = ""
        self.ignorepgHbaMiss = False
        self.clusterConf = ""
        self.logFile = ""
        self.removeIps = []
        self.addIps = []
        self.dws_mode = False


def usage():
    """
ConfigHba.py is a utility to configure pg_hba file on all nodes.

Usage:
  python3 ConfigHba.py --help
  python3 ConfigHba.py -U USER
  [-X XMLFILE] [-l LOGFILE]
  [-r] [--remove-ip IPADDRESS [...]] [--add-ip=IPADDRESS]

General options:
  -U                                 Cluster user.
  -X                                 Path of the XML configuration file.
  -l                                 Path of log file.
  -r                                 the signal about ignorepgHbaMiss
  --remove-ip                        Remove ip address from pg_hba.conf
  --add-ip                           Add ip address to pg_hba.conf
  --help                             Show help information for this utility,
                                     and exit the command line mode.
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: parse command line
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:X:l:r",
                                   ["remove-ip=", "help", "dws-mode",
                                    "add-ip="])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

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
        elif (key == "-X"):
            g_opts.clusterConf = value
        elif (key == "-l"):
            g_opts.logFile = value
        elif (key == "-r"):
            g_opts.ignorepgHbaMiss = True
        elif (key == "--remove-ip"):
            g_opts.removeIps.append(value)
        elif (key == "--dws-mode"):
            g_opts.dws_mode = True
        elif (key == "--add-ip"):
            g_opts.addIps = value.split(',')
        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: check parameter
    """
    if (g_opts.clusterUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")

    if g_opts.ignorepgHbaMiss:
        gaussHome = DefaultValue.getEnv("GAUSSHOME")
        if not gaussHome:
            GaussLog.exitWithError(ErrorCode.GAUSS_518["GAUSS_51802"]
                                   % "GAUSSHOME")
        staticConfigfile = "%s/bin/cluster_static_config" % gaussHome
        if (not os.path.isfile(staticConfigfile)):
            GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50210"]
                                   % staticConfigfile)

    if (g_opts.clusterConf != ""):
        if (not os.path.exists(g_opts.clusterConf)):
            GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50201"]
                                   % g_opts.clusterConf)

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, g_opts.clusterUser, "")


class ConfigHba(LocalBaseOM):
    """
    class: configHba
    """

    def __init__(self, logFile, user, clusterConf, dwsMode=False,
                 ignorepgHbaMiss=False, removeIps=None):
        """
        function: configure all instance on local node
        """
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
        # init every component
        self.initComponent()

        self.ignorepgHbaMiss = ignorepgHbaMiss
        self.allIps = []
        if removeIps is None:
            removeIps = []
        self.removeIps = removeIps

    def getAllIps(self):
        """
        function: get all ip info from static configuration file
        input : NA
        output: NA
        """
        if (g_opts.addIps):
            self.allIps = g_opts.addIps
            return

        # get all node names
        nodenames = self.clusterInfo.getClusterNodeNames()
        for nodename in nodenames:
            nodeinfo = self.clusterInfo.getDbNodeByName(nodename)
            self.allIps += nodeinfo.backIps
            self.allIps += nodeinfo.sshIps
            for inst in nodeinfo.datanodes:
                self.allIps += inst.haIps
                self.allIps += inst.listenIps
        # get all ips. Remove the duplicates ips 
        self.allIps = DefaultValue.Deduplication(self.allIps)

    def configHba(self):
        """
        function: set hba config
        input : NA
        output: NA
        """
        self.getAllIps()
        componentList = self.dnCons
        # Determine whether this node containing CN, DN instance
        if (len(componentList) == 0):
            return
        try:
            parallelTool.parallelExecute(self.__configAnInstance,
                                         componentList)
            self.logger.log("Successfully configured all instances"
                            " on node[%s]." % DefaultValue.GetHostIpOrName())
        except Exception as e:
            raise Exception(str(e))

    def __configAnInstance(self, component):
        """
        function: set hba config for single component
        input : component
        output: NA
        """
        # check instance data directory
        if (component.instInfo.datadir == "" or
                not os.path.exists(component.instInfo.datadir)):
            if self.ignorepgHbaMiss:
                self.logger.debug("Failed to obtain data directory of"
                                  " the instance[%s]."
                                  % str(component.instInfo))
                return
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"]
                                % ("data directory of the instance[%s]"
                                   % str(component.instInfo)))

        # check pg_hba.conf
        hbaFile = "%s/pg_hba.conf" % component.instInfo.datadir
        if self.ignorepgHbaMiss and not os.path.exists(hbaFile):
            self.logger.debug("The %s does not exist." % hbaFile)
            return

        component.setPghbaConfig(self.allIps)
        if len(self.removeIps) != 0:
            component.removeIpInfoOnPghbaConfig(self.removeIps)


if __name__ == '__main__':
    """
    function: config database node instance hba.conf
              1.check dbInitParams
              2.config instance hba.conf
    input : NA
    output: NA
    """
    try:
        # parse and check input parameters
        parseCommandLine()
        checkParameter()

        # modify Instance 
        configer = ConfigHba(g_opts.logFile, g_opts.clusterUser,
                             g_opts.clusterConf, g_opts.dws_mode,
                             g_opts.ignorepgHbaMiss, g_opts.removeIps)
        configer.configHba()

    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
