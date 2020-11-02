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
# Description  : gs_install is a utility to deploy a Gauss200 server.
#############################################################################
import os
import sys

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from gspylib.os.gsfile import g_file
from gspylib.common.DbClusterInfo import dbNodeInfo, \
    dbClusterInfo, compareObject

#############################################################################
# Const variables
#   INSTALL_STEP: the signal about install
#   STEPBACKUP_DIR: the backup directory storage step information
#   STEP_INIT: the signal about install
#   STEP_INSTALL: the signal about install
#   STEP_CONFIG: the signal about install
#   STEP_START: the signal about install
#############################################################################
INSTALL_STEP = ""
STEPBACKUP_DIR = ""
STEP_INIT = "Init Install"
STEP_INSTALL = "Install cluster"
STEP_CONFIG = "Config cluster"
STEP_START = "Start cluster"

#############################################################################
# TP cluster type
#############################################################################

#####################################################
# Ation type
#####################################################
ACTION_INSTALL_CLUSTER = "install_cluster"
ACTION_START_CLUSTER = "start_cluster"
ACTION_BUILD_STANDBY = "build_standby"
ACTION_BUILD_CASCADESTANDBY = "build_cascadestandby"

# exit code
EXEC_SUCCESS = 0


#############################################################################
# Global variables
#   self.context.logger: globle logger
#   self.context.clusterInfo: global clueter information
#   self.context.sshTool: globle ssh tool interface
#############################################################################

class InstallImpl:
    """
    The class is used to do perform installation
    """
    """
    init the command options
    save command line parameter values
    """

    def __init__(self, install):
        """
        function: constructor
        """
        self.context = install

    def run(self):
        """
        function: run method
        """
        try:
            # check timeout time.
            # Notice: time_out is not supported under TP branch
            self.checkTimeout()

            # check if have done preinstall for this user on every node
            self.checkGaussenvFlag()
            # check the clueter status
            self.checkClusterStatus()
            # creating the backup directory
            self.prepareBackDir()
            # Check time consistency(only TP use it must less 2s)
            self.checkTimeConsistency()
            # install clueter
            self.context.logger.log("begin deploy..")
            self.doDeploy()
            self.context.logger.log("end deploy..")
            # close the log file
            self.context.logger.closeLog()
        except Exception as e:
            GaussLog.exitWithError(str(e))

    def checkTimeout(self):
        """
        function: check timeout
        """
        pass

    def checkGaussenvFlag(self):
        """
        function: check if have done preinstall for this user on every node
        1 PREINSTALL_FLAG
        2 INSTALL_FLAG
        input : NA
        output: NA
        """
        try:
            self.context.logger.log("Check preinstall on every node.",
                                    "addStep")
            self.context.checkPreInstall(self.context.user, "preinstall")
            self.context.logger.log(
                "Successfully checked preinstall on every node.", "constant")
        except Exception as e:
            self.context.logger.logExit(str(e))

    def checkClusterStatus(self):
        """
        function: Check if cluster is running
        input : NA
        output: NA
        """
        pass

    def checkTimeConsistency(self):
        """
        Check time consistency between hosts in cluster
        :return: NA
        """
        pass

    def prepareBackDir(self):
        """
        function: Creating the backup directory
        input : NA
        output: NA
        """
        self.context.logger.log("Creating the backup directory.", "addStep")
        self.context.managerOperateStepDir()

        # if INSTALL_STEP is exists
        if (os.path.exists(self.context.operateStepFile)):
            # read the step from INSTALL_STEP
            warmstep = self.context.readOperateStep()
            # print the step
            self.context.logger.log("Last time end with %s." % warmstep)
            self.context.logger.log("Continue this step.")

        # Successfully created the backup directory
        self.context.logger.log("Successfully created the backup directory.",
                                "constant")

    def checkPgLogFileMode(self):
        """
        function: change pg_log file mode
        input : NA
        output: NA
        """
        pass

    def compareOldNewClusterConfigInfo(self, clusterInfo, oldClusterInfo):
        """
        function: verify cluster config info between old and new cluster
        input : clusterInfo, oldClusterInfo
        output: NA
        """

        # covert new cluster information to compare cluster
        compnew = self.storageDbClusterInfo(clusterInfo)
        # covert old cluster information to compare cluster
        compold = self.storageDbClusterInfo(oldClusterInfo)
        # do compare
        # if it is not same, print it.
        theSame, tempbuffer = compareObject(compnew, compold, "clusterInfo",
                                            [])
        if (theSame):
            self.context.logger.debug(
                "Static configuration matched with old "
                "static configuration files.")
        else:
            msg = \
                "Instance[%s] are not the same." \
                "\nXmlConfigFile:    %s\nStaticConfigFile: %s\n" % \
                (tempbuffer[0], tempbuffer[1], tempbuffer[2])
            self.context.logger.log(
                "The cluster's static configuration "
                "does not match the new configuration file.")
            self.context.logger.log(msg.strip("\n"))
        return theSame

    def storageDbClusterInfo(self, dbclusterInfo):
        """
        function: covert to comp cluster
        input : dbclusterInfo
        output: midClusterInfo
        """
        # init dbcluster class
        midClusterInfo = dbClusterInfo()
        # get cluster name
        midClusterInfo.name = dbclusterInfo.name
        for dbnode in dbclusterInfo.dbNodes:
            compNodeInfo = dbNodeInfo()
            compNodeInfo.azName = dbnode.azName
            compNodeInfo.name = dbnode.name
            midClusterInfo.dbNodes.append(compNodeInfo)
        return midClusterInfo

    def doDeploy(self):
        """
        function: Deploy Application
        input : NA
        output: NA
        """
        # read the install setp from INSTALL_STEP
        self.context.logger.debug("Installing application")
        # compare xmlconfigInfo with staticConfigInfo
        gaussHome = DefaultValue.getInstallDir(self.context.user)
        commonStaticConfigFile = "%s/bin/cluster_static_config" % gaussHome
        if os.path.exists(commonStaticConfigFile):
            self.context.oldClusterInfo = dbClusterInfo()
            self.context.oldClusterInfo.initFromStaticConfig(
                self.context.user,
                commonStaticConfigFile)
            sameFlag = self.compareOldNewClusterConfigInfo(
                self.context.clusterInfo, self.context.oldClusterInfo)
        else:
            sameFlag = True

        step = self.context.readOperateStep()
        # if step is STEP_INSTALL
        if (step == STEP_INSTALL) or (step == STEP_CONFIG and not sameFlag):
            # rollback the install
            self.rollbackInstall()
            # write the install step
            self.context.writeOperateStep(STEP_INIT)

        # read the install step from INSTALL_STEP
        step = self.context.readOperateStep()
        # if step is STEP_INIT
        if step == STEP_INIT:
            # write the install step STEP_INSTALL into INSTALL_STEP
            self.context.writeOperateStep(STEP_INSTALL)
            # install Gauss200 DB
            self.doInstall()
            # write the install step STEP_CONFIG into INSTALL_STEP
            self.context.writeOperateStep(STEP_CONFIG)

        # read the install step from INSTALL_STEP
        step = self.context.readOperateStep()
        # if step is STEP_CONFIG
        if step == STEP_CONFIG:
            # config Gauss200 DB
            self.doConfig()
            # write the install step STEP_CONFIG into STEP_START
            self.context.writeOperateStep(STEP_START)

        # read the install step from INSTALL_STEP
        step = self.context.readOperateStep()
        # if step is STEP_START
        if step == STEP_START:
            # start Gauss200 DB
            self.doStart()
            # change pg_log file mode in pg_log path (only AP)
            self.checkPgLogFileMode()

        # clear the backup directory.
        self.context.managerOperateStepDir("delete")
        self.context.logger.log("Successfully installed application.")

    def prepareInstallCluster(self):
        """
        prepared install cluster
        AP: distribute package
        and Check installation environment on all nodes
        TP: skip
        """
        pass

    def installClusterApp(self):
        """
        function: install cluster instance
        input : NA
        output: NA
        """
        self.context.logger.log("Installing applications on all nodes.")
        # Installing applications
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -t %s -U %s -X %s -R %s -c %s -l %s %s" % (
            OMCommand.getLocalScript("Local_Install"),
            ACTION_INSTALL_CLUSTER,
            self.context.user + ":" + self.context.group,
            self.context.xmlFile,
            self.context.clusterInfo.appPath, self.context.clusterInfo.name,
            self.context.localLog,
            self.getCommandOptions())
        self.context.logger.debug(
            "Command for installing application: %s" % cmd)

        # exec the cmd for install application on all nodes
        DefaultValue.execCommandWithMode(cmd,
                                         "Install applications",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)
        self.context.logger.log("Successfully installed APP.")

    def doInstall(self):
        """
        function: do install
        input: NA
        output: NA
        """
        self.context.logger.log("Installing the cluster.", "addStep")
        try:
            # prepared install cluster
            # AP: distribute package
            print("begin prepare Install Cluster..")
            self.prepareInstallCluster()
        except Exception as e:
            self.context.managerOperateStepDir("delete")
            self.context.logger.logExit(str(e))

        try:
            # install cluster APP
            # AP: 1. check env 2. tar -xvcf PAKCAGE 3. modefy env flag
            self.context.logger.log("begin install Cluster..")
            self.installClusterApp()
            self.context.logger.log("begin init Instance..")
            self.initInstance()
            self.configZenithInst()
            self.context.logger.log("encrypt cipher and rand files "
                                    "for database.")
            self.context.genCipherAndRandFile()
            self.context.logger.log("begin to create CA cert files")
            self.context.createServerCa()
            if not self.context.localMode:
                self.context.createGrpcCa()

        except Exception as e:
            self.context.logger.logExit(str(e))

        # Cluster installation is completed
        self.context.logger.log("Cluster installation is completed.",
                                "constant")

    def configZenithInst(self):
        """
        function: config zenith inst
        :return:
        """
        pass

    def initInstance(self):
        """
        function: init instance
        :return:
        """
        pass

    def getCommandOptions(self):
        """
        function: get command options
        """
        pass

    def checkNodeConfig(self):
        """
        function: Check node config on all nodes
        input : NA
        output: NA
        """
        pass

    def distributeEncryptFiles(self):
        """
        function: distribute encrypt files
        """
        pass

    # for ap
    def prepareConfigCluster(self):
        """
        function: install cluster instance
        input : NA
        output: NA
        """
        pass

    def initNodeInstance(self):
        """
        function: init instance applications
        input : NA
        output: NA
        """
        pass

    def configInstance(self):
        """
        function: config instance
        input : NA
        output: NA
        """
        pass

    def distributeRackInfo(self):
        """
        function: Distributing the rack Information File
        input : NA
        output: NA
        """
        pass

    def doConfig(self):
        """
        function: Do config action
        input : NA
        output: NA
        """
        self.context.logger.log("Configuring.", "addStep")
        try:
            # prepared config cluster
            # AP: clean instance directory and check node config
            self.prepareConfigCluster()
            self.initNodeInstance()
            self.configInstance()
            self.distributeRackInfo()
            DefaultValue.enableWhiteList(
                self.context.sshTool,
                self.context.mpprcFile,
                self.context.clusterInfo.getClusterNodeNames(),
                self.context.logger)
        except Exception as e:
            # failed to clear the backup directory
            self.context.logger.logExit(str(e))
        # Configuration is completed
        self.context.logger.log("Configuration is completed.", "constant")

    def startCluster(self):
        """
        function: start cluster
        input : NA
        output: NA
        """
        # Start cluster applications
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -t %s -U %s -X %s -R %s -c %s -l %s %s" % (
            OMCommand.getLocalScript("Local_Install"),
            ACTION_START_CLUSTER,
            self.context.user + ":" + self.context.group,
            self.context.xmlFile,
            self.context.clusterInfo.appPath,
            self.context.clusterInfo.name, self.context.localLog,
            self.getCommandOptions())
        self.context.logger.debug("Command for start cluster: %s" % cmd)
        DefaultValue.execCommandWithMode(
            cmd,
            "Start cluster",
            self.context.sshTool,
            self.context.isSingle or self.context.localMode,
            self.context.mpprcFile)

        # build stand by
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -t %s -U %s -X %s -R %s -c %s -l %s %s" % (
            OMCommand.getLocalScript("Local_Install"),
            ACTION_BUILD_STANDBY,
            self.context.user + ":" + self.context.group,
            self.context.xmlFile,
            self.context.clusterInfo.appPath,
            self.context.clusterInfo.name, self.context.localLog,
            self.getCommandOptions())
        self.context.logger.debug("Command for build standby: %s" % cmd)
        DefaultValue.execCommandWithMode(
            cmd,
            "Build standby",
            self.context.sshTool,
            self.context.isSingle or self.context.localMode,
            self.context.mpprcFile)

        # build casecadestand by
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -t %s -U %s -X %s -R %s -c %s -l %s %s" % (
            OMCommand.getLocalScript("Local_Install"),
            ACTION_BUILD_CASCADESTANDBY,
            self.context.user + ":" + self.context.group,
            self.context.xmlFile,
            self.context.clusterInfo.appPath,
            self.context.clusterInfo.name, self.context.localLog,
            self.getCommandOptions())
        self.context.logger.debug("Command for build cascade standby: %s" % cmd)
        for hostname in self.context.sshTool.hostNames:
            DefaultValue.execCommandWithMode(
                cmd,
                "Build cascade standby",
                self.context.sshTool,
                self.context.isSingle or self.context.localMode,
                self.context.mpprcFile, [hostname])

        self.context.logger.log("Successfully started cluster.")

    def doStart(self):
        """
        function:start cluster
        input : NA
        output: NA
        """
        self.context.logger.debug("Start the cluster.", "addStep")
        try:
            tmpGucFile = ""
            tmpGucPath = DefaultValue.getTmpDirFromEnv(self.context.user)
            tmpGucFile = "%s/tmp_guc" % tmpGucPath
            cmd = g_file.SHELL_CMD_DICT["deleteFile"] % (
                tmpGucFile, tmpGucFile)
            DefaultValue.execCommandWithMode(cmd, "Install applications",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            # start cluster in non-native mode
            self.startCluster()
        except Exception as e:
            self.context.logger.logExit(str(e))
        self.context.logger.debug("Successfully started the cluster.",
                                  "constant")

    def rollbackInstall(self):
        """
        function: Rollback install
        input : NA
        output: NA
        0 succeed
        1 failed
        2 rollback succeed
        3 rollback failed
        """
        pass

    # for olap
    def deleteTempFileForUninstall(self):
        """
        function: Rollback install ,delete temporary file
        input : NA
        output: NA
        """
        pass
