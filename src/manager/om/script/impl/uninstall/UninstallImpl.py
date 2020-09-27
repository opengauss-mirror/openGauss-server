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
import subprocess
import time
import os

sys.path.append(sys.path[0] + "/../")

from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file


class UninstallImpl:
    """
    init the command options
    save command line parameter values
    """

    def __init__(self, unstallation):
        """
        function: constructor
        """
        pass

    def checkLogFilePath(self):
        """
        function: Check log file path
        input : NA
        output: NA
        """
        clusterPath = []
        try:
            # get tool path
            clusterPath.append(DefaultValue.getClusterToolPath())
            # get tmp path
            tmpDir = DefaultValue.getTmpDirFromEnv()
            clusterPath.append(tmpDir)
            # get cluster path
            hostName = DefaultValue.GetHostIpOrName()
            dirs = self.clusterInfo.getClusterDirectorys(hostName, False)
            # loop all cluster path
            for checkdir in dirs.values():
                clusterPath.extend(checkdir)
            self.logger.debug("Cluster paths %s." % clusterPath)

            # check directory
            g_file.checkIsInDirectory(self.logFile, clusterPath)
        except Exception as e:
            self.logger.logExit(str(e))

    def checkUninstall(self):
        """
        function: Check uninstall
        input : NA
        output: NA
        """
        # Checking uninstallation
        self.logger.log("Checking uninstallation.", "addStep")
        # use check uninstall to check every nodes
        cmd = "%s -R '%s' -U %s -l %s" % (
            OMCommand.getLocalScript("Local_Check_Uninstall"),
            self.clusterInfo.appPath, self.user, self.localLog)
        # check if need to clean instance
        if (self.cleanInstance):
            cmd += " -d"
        self.logger.debug("Command for checking uninstallation: " + cmd)
        DefaultValue.execCommandWithMode(cmd, "check uninstallation.",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)
        self.logger.log("Successfully checked uninstallation.", "constant")

    def StopCluster(self):
        """
        function: Stopping the cluster
        input : NA
        output: NA
        """
        self.logger.log("Stopping the cluster.", "addStep")
        # get the static config
        static_config = \
            "%s/bin/cluster_static_config" % self.clusterInfo.appPath
        static_config_bak = \
            "%s/bin/cluster_static_config_bak" % self.clusterInfo.appPath
        # if cluster_static_config_bak exists
        # and static_config does not exists, mv it to static_config
        if (not os.path.exists(static_config) and os.path.exists(
                static_config_bak)):
            cmd = "mv %s %s" % (static_config_bak, static_config)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.debug("The cmd is %s " % cmd)
                self.logger.error("rename cluster_static_config_bak failed")
                self.logger.debug("Error:\n%s" % output)
        # if path not exits, can not stop cluster
        if (not os.path.exists(static_config)):
            self.logger.debug("Failed to stop the cluster.", "constant")
            return

        # Stop cluster applications
        cmd = "source %s; %s -U %s -R %s -l %s" % (
            self.mpprcFile, OMCommand.getLocalScript("Local_StopInstance"),
            self.user, self.clusterInfo.appPath, self.localLog)
        self.logger.debug("Command for stop cluster: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd, "Stop cluster", self.sshTool,
                                         self.localMode, self.mpprcFile)
        self.logger.log("Successfully stopped cluster.")

    def CheckAndKillAliveProc(self, procFileName):
        """
        function: When uninstall gaussdb cluster. After it is stopped,
                  We must make sure that all process
                  about gaussdb cluster have been stopped. Not including
                  om_monitor.
        input : procFileName
        output: NA
        """
        try:
            failedNodes = []
            validNodeName = self.clusterInfo.getClusterNodeNames()
            # the command for killing all process
            cmd_check_kill = DefaultValue.killInstProcessCmd(procFileName,
                                                             True, 9, False)
            # use sshTool to kill process in all nodes
            (status, output) = self.sshTool.getSshStatusOutput(cmd_check_kill,
                                                               validNodeName)
            # get the node which not be killed
            for node in validNodeName:
                if (status[node] != DefaultValue.SUCCESS):
                    failedNodes.append(node)
            # kill process in nodes again
            if (len(failedNodes)):
                time.sleep(1)
                (status, output) = self.sshTool.getSshStatusOutput(
                    cmd_check_kill, failedNodes)
                for node in failedNodes:
                    # if still fail, throw error
                    if (status[node] != DefaultValue.SUCCESS):
                        raise Exception(output)

        except Exception as e:
            raise Exception(str(e))

    def CleanInstance(self):
        """
        function: clean instance
        input  : NA
        output : NA
        """
        self.logger.debug("Deleting instance.", "addStep")
        # check if need delete instance
        if (not self.cleanInstance):
            self.logger.debug("No need to delete data.", "constant")
            return

        # Clean instance data
        cmd = "%s -U %s -l %s" % (
            OMCommand.getLocalScript("Local_Clean_Instance"), self.user,
            self.localLog)
        self.logger.debug("Command for deleting instance: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd, "delete instances data.",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)

        # clean upgrade temp backup path
        upgrade_bak_dir = DefaultValue.getBackupDir("upgrade")
        cmd = g_file.SHELL_CMD_DICT["cleanDir"] % (
            upgrade_bak_dir, upgrade_bak_dir, upgrade_bak_dir)
        DefaultValue.execCommandWithMode(cmd,
                                         "delete backup directory for upgrade",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)

        self.logger.log("Successfully deleted instances.", "constant")

    def CleanTmpFiles(self):
        """
        function: clean temp files
        input : NA
        output: NA
        """
        self.logger.debug("Deleting temporary files.", "addStep")
        try:
            # copy record_app_directory file
            tmpDir = DefaultValue.getTmpDirFromEnv(self.user)
            if tmpDir == "":
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$PGHOST")
            upgradeBackupPath = os.path.join(tmpDir, "binary_upgrade")
            copyPath = os.path.join(upgradeBackupPath, "record_app_directory")
            appPath = DefaultValue.getInstallDir(self.user)
            if appPath == "":
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$PGHOST")
            if copyPath != "":
                copyCmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s/';fi)" % (
                    copyPath, copyPath, appPath)
                DefaultValue.execCommandWithMode(
                    copyCmd,
                    "copy record_app_directory file",
                    self.sshTool, self.localMode,
                    self.mpprcFile)

            cmd = g_file.SHELL_CMD_DICT["cleanDir"] % (
                self.tmpDir, self.tmpDir, self.tmpDir)
            # clean dir of PGHOST
            DefaultValue.execCommandWithMode(cmd, "delete temporary files",
                                             self.sshTool, self.localMode,
                                             self.mpprcFile)
        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.debug("Successfully deleted temporary files.", "constant")

    def UninstallApp(self):
        """
        function: Uninstall application
        input : NA
        output: NA
        """
        self.logger.log("Uninstalling application.", "addStep")
        cmd = "%s -R '%s' -U %s -l %s -T" % (
            OMCommand.getLocalScript("Local_Uninstall"),
            self.clusterInfo.appPath,
            self.user, self.localLog)
        self.logger.debug("Command for Uninstalling: %s" % cmd)
        # clean application
        DefaultValue.execCommandWithMode(cmd, "uninstall application",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)
        self.logger.log("Successfully uninstalled application.", "constant")

    def CleanStaticConfFile(self):
        """
        function: clean static conf file
        input : NA
        output: NA
        """
        self.logger.debug("Deleting static configuration file.", "addStep")
        try:
            cmd = "rm -rf '%s'/bin " % self.clusterInfo.appPath
            # delete bin dir in GAUSSHOME
            DefaultValue.execCommandWithMode(
                cmd,
                "delete cluster static configuration file.",
                self.sshTool, self.localMode,
                self.mpprcFile)
        except Exception as e:
            self.logger.exitWithError(str(e))
        self.logger.debug("Successfully deleted static configuration file.",
                          "constant")

    def CleanRackFile(self):
        """
        function: clean rack information file
        input : NA
        output: NA
        """
        gp_home = DefaultValue.getEnv("GPHOME")
        if os.path.exists(gp_home):
            gp_home = os.path.realpath(gp_home)
        rack_conf_file = os.path.realpath(
            os.path.join(gp_home, "script/gspylib/etc/conf/rack_info.conf"))
        if os.path.isfile(rack_conf_file):
            cmd = "rm -f %s" % rack_conf_file
            DefaultValue.execCommandWithMode(cmd,
                                             "Deleted rack information file.",
                                             self.sshTool, self.localMode,
                                             mpprcFile=self.mpprcFile)
            self.logger.debug("Successfully deleted rack information file.")

    def CleanLog(self):
        """
        function: Clean default log
        input : NA
        output: NA
        """
        self.logger.debug("Deleting log.", "addStep")
        # check if need delete instance
        if (not self.cleanInstance):
            self.logger.debug("No need to delete data.", "constant")
            return

        try:
            # clean log
            userLogDir = DefaultValue.getUserLogDirWithUser(self.user)
            cmd = g_file.SHELL_CMD_DICT["cleanDir"] % (
                userLogDir, userLogDir, userLogDir)
            # delete log dir
            DefaultValue.execCommandWithMode(cmd, "delete user log directory",
                                             self.sshTool, self.localMode,
                                             self.mpprcFile)
        except Exception as e:
            self.logger.exitWithError(str(e))
        self.logger.debug("Successfully deleted log.", "constant")

    def checkEnv(self):
        """
        function: check if GAUSS_ENV is 2
        input : NA
        output: NA
        """
        pass

    def ReCleanEtcdPath(self):
        """
        function: make sure the etcd path is clean.
        input : NA
        output: NA
        """
        pass

    def ReKillEtcdProcess(self):
        """
        function: make sure the etcd process is clean.
        input : NA
        output: NA
        """
        if (self.localMode):
            DefaultValue.KillAllProcess(self.user, "etcd")
        # kill process in all nodes
        else:
            etcd_file = "%s/bin/etcd" % self.clusterInfo.appPath
            self.CheckAndKillAliveProc(etcd_file)

    def run(self):
        """
        function: Uninstall database cluster
        input : NA
        output: NA
        """
        try:
            self.checkEnv()
            self.checkLogFilePath()
            # do uninstall
            self.checkUninstall()
            self.StopCluster()
            self.CleanInstance()
            self.CleanTmpFiles()
            self.UninstallApp()
            self.ReCleanEtcdPath()
            self.ReKillEtcdProcess()
            self.logger.closeLog()
            self.CleanStaticConfFile()
            self.CleanRackFile()
            self.CleanLog()
            self.logger.log("Uninstallation succeeded.")
        except Exception as e:
            self.logger.logExit(str(e))
