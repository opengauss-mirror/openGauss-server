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

import os
import sys
import subprocess
import grp
import pwd
import getpass

sys.path.append(sys.path[0] + "/../")
from gspylib.threads.parallelTool import parallelTool
from gspylib.common.DbClusterInfo import initParserXMLFile, \
    readOneClusterConfigItem
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
from gspylib.os.gsfile import g_Platform
from gspylib.common.VersionInfo import VersionInfo

sys.path.append(sys.path[0] + "/../../../lib/")
DefaultValue.doConfigForParamiko()
import paramiko

#############################################################################
# Global variables
#############################################################################
gphome = None
# system config file
PROFILE_FILE = '/etc/profile'
# pssh directory name
PSSHDIR = 'pssh-2.3.1'
# action name
ACTION_CLEAN_TOOL_ENV = "clean_tool_env"
ACTION_CHECK_UNPREINSTALL = "check_unpreinstall"
ACTION_CLEAN_GAUSS_ENV = "clean_gauss_env"
ACTION_DELETE_GROUP = "delete_group"
ACTION_CLEAN_SYSLOG_CONFIG = 'clean_syslog_config'
ACTION_CLEAN_DEPENDENCY = "clean_dependency"


class PostUninstallImpl:
    """
    init the command options
    input : NA
    output: NA
    """

    def __init__(self, GaussPost):
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
            self.logger.log("Check log file path.", "addStep")
            # get tool path
            clusterPath.append(DefaultValue.getClusterToolPath())

            # get tmp path
            tmpDir = DefaultValue.getTmpDir(self.user, self.xmlFile)
            clusterPath.append(tmpDir)

            # get cluster  path
            hostName = DefaultValue.GetHostIpOrName()
            dirs = self.clusterInfo.getClusterDirectorys(hostName, False)
            for checkdir in dirs.values():
                clusterPath.extend(checkdir)

            self.logger.debug("Cluster paths %s." % clusterPath)
            # check directory
            g_file.checkIsInDirectory(self.logFile, clusterPath)
            self.logger.log("Successfully checked log file path.", "constant")
        except Exception as e:
            self.logger.logExit(str(e))

    ##########################################################################
    # Uninstall functions
    ##########################################################################
    def doCleanEnvironment(self):
        """
        function: Clean Environment
        input : NA
        output: NA
        """
        self.logger.debug("Do clean Environment.", "addStep")
        try:
            # check uninstall
            self.checkUnPreInstall()
            # clean app/log/data/temp dirs
            self.cleanDirectory()
            # clean other user
            self.cleanRemoteOsUser()
            # clean other nodes environment software and variable
            self.cleanOtherNodesEnvSoftware()
            # clean other nodes log
            self.cleanOtherNodesLog()
            # clean local node environment software and variable
            self.cleanLocalNodeEnvSoftware()
            # clean local user
            self.cleanLocalOsUser()
        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.debug("Do clean Environment succeeded.", "constant")

    def checkUnPreInstall(self):
        """
        function: check whether do uninstall before unpreinstall
        input : NA
        output: NA
        """
        self.logger.log("Checking unpreinstallation.")
        if not self.localMode:
            DefaultValue.checkAllNodesMpprcFile(
                self.clusterInfo.getClusterNodeNames(),
                self.clusterInfo.appPath, self.mpprcFile)

        cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
            OMCommand.getLocalScript("Local_UnPreInstall"),
            ACTION_CHECK_UNPREINSTALL,
            self.user,
            self.localLog,
            self.xmlFile)
        self.logger.debug("Command for checking unpreinstall: %s" % cmd)
        # check if do postuninstall in all nodes
        DefaultValue.execCommandWithMode(cmd, "check unpreinstall",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)
        self.logger.log("Successfully checked unpreinstallation.")

    def cleanDirectory(self):
        """
        function: clean install/instance/temp dirs
        input : NA
        output: NA
        """
        # clean instance path
        hostName = DefaultValue.GetHostIpOrName()
        dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
        instanceDirs = []
        # get DB instance
        for dbInst in dbNodeInfo.datanodes:
            instanceDirs.append(dbInst.datadir)
            if (len(dbInst.ssdDir) != 0):
                instanceDirs.append(dbInst.ssdDir)
        # clean all instances
        if (len(instanceDirs) > 0):
            if (os.path.exists(instanceDirs[0]) and len(
                    os.listdir(instanceDirs[0])) == 0):
                self.CleanInstanceDir()
            else:
                self.logger.debug(
                    "Instance directory [%s] is not empty. "
                    "Skip to delete instance's directory." %
                    instanceDirs[0])
        else:
            self.logger.debug(
                "Instance's directory is not been found. "
                "Skip to delete instance's directory.")

        # clean install path
        if (os.path.exists(self.clusterInfo.appPath)):
            self.logger.log("Deleting the installation directory.")
            cmd = "rm -rf '%s'" % self.clusterInfo.appPath
            self.logger.debug(
                "Command for deleting the installation path: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd, "delete install path",
                                             self.sshTool, self.localMode,
                                             self.mpprcFile)
            self.logger.log("Successfully deleted the installation directory.")

        # clean tmp dir
        self.logger.log("Deleting the temporary directory.")
        tmpDir = DefaultValue.getTmpDir(self.user, self.xmlFile)
        cmd = "rm -rf '%s'; rm -rf /tmp/gs_checkos; rm -rf /tmp/gs_virtualip" \
              % tmpDir
        self.logger.debug(
            "Command for deleting the temporary directory: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd, "delete the temporary directory",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)
        self.logger.log("Successfully deleted the temporary directory.")

    def CleanInstanceDir(self):
        """
        function: Clean instance directory
        input : NA
        output: NA
        """
        self.logger.log("Deleting the instance's directory.")
        cmd = "%s -U %s -l '%s' -X '%s'" % (
            OMCommand.getLocalScript("Local_Clean_Instance"), self.user,
            self.localLog, self.xmlFile)
        self.logger.debug("Command for deleting the instance: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd, "delete the instances data",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)

        # clean upgrade temp backup path
        cmd = "rm -rf '%s'" % DefaultValue.getBackupDir("upgrade")
        self.logger.debug(
            "Command for deleting the upgrade temp backup path: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "delete backup directory for upgrade",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)

        self.logger.log("Successfully deleted the instance's directory.")

    def cleanRemoteOsUser(self):
        """
        function: Clean remote os user
        input : NA
        output: NA
        """
        # check if local mode
        if (self.localMode):
            return

        if (not self.deleteUser):
            # clean static config file
            cmd = "rm -rf '%s'" % self.clusterInfo.appPath
            DefaultValue.execCommandWithMode(cmd, "delete install directory",
                                             self.sshTool, self.localMode,
                                             self.mpprcFile)
            return

        group = grp.getgrgid(pwd.getpwnam(self.user).pw_gid).gr_name

        # get other nodes
        hostName = DefaultValue.GetHostIpOrName()
        otherNodes = self.clusterInfo.getClusterNodeNames()
        for otherNode in otherNodes:
            if (otherNode == hostName):
                otherNodes.remove(otherNode)

        # clean remote user
        self.logger.log("Deleting remote OS user.")
        cmd = "%s -U %s -l %s" % (
            OMCommand.getLocalScript("Local_Clean_OsUser"), self.user,
            self.localLog)
        self.logger.debug("Command for deleting remote OS user: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd, "delete OS user", self.sshTool,
                                         self.localMode, self.mpprcFile,
                                         otherNodes)
        self.logger.log("Successfully deleted remote OS user.")

        if (self.deleteGroup):
            # clean remote group
            self.logger.debug("Deleting remote OS group.")
            cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
                OMCommand.getLocalScript("Local_UnPreInstall"),
                ACTION_DELETE_GROUP, group, self.localLog, self.xmlFile)
            self.logger.debug("Command for deleting remote OS group: %s" % cmd)
            status = self.sshTool.getSshStatusOutput(cmd, otherNodes,
                                                               self.mpprcFile)[0]
            outputMap = self.sshTool.parseSshOutput(otherNodes)
            for node in status.keys():
                if (status[node] != DefaultValue.SUCCESS):
                    self.logger.log((outputMap[node]).strip("\n"))
            self.logger.debug("Deleting remote group is completed.")

    def cleanOtherNodesEnvSoftware(self):
        """
        function: clean other nodes environment software and variable
        input : NA
        output: NA
        """
        # check if local mode
        if self.localMode:
            return
        self.logger.log(
            "Deleting software packages "
            "and environmental variables of other nodes.")
        try:
            # get other nodes
            hostName = DefaultValue.GetHostIpOrName()
            otherNodes = self.clusterInfo.getClusterNodeNames()
            for otherNode in otherNodes:
                if (otherNode == hostName):
                    otherNodes.remove(otherNode)
            self.logger.debug(
                "Deleting environmental variables of nodes: %s." % otherNodes)

            # clean $GAUSS_ENV
            if (not self.deleteUser):
                cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
                    OMCommand.getLocalScript("Local_UnPreInstall"),
                    ACTION_CLEAN_GAUSS_ENV,
                    self.user,
                    self.localLog,
                    self.xmlFile)
                self.logger.debug("Command for deleting $GAUSS_ENV: %s" % cmd)
                DefaultValue.execCommandWithMode(cmd, "delete $GAUSS_ENV",
                                                 self.sshTool, self.localMode,
                                                 self.mpprcFile, otherNodes)
            cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
                OMCommand.getLocalScript("Local_UnPreInstall"),
                ACTION_CLEAN_TOOL_ENV,
                self.user,
                self.localLog,
                self.xmlFile)
            self.logger.debug(
                "Command for deleting environmental variables: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "delete environment variables",
                                             self.sshTool,
                                             self.localMode,
                                             self.mpprcFile,
                                             otherNodes)
        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.log(
            "Successfully deleted software packages "
            "and environmental variables of other nodes.")

    def cleanOtherNodesLog(self):
        """
        function: clean other nodes log
        input : NA
        output: NA
        """
        # check if local mode
        if self.localMode:
            return
        self.logger.log("Deleting logs of other nodes.")
        try:
            # get other nodes
            hostName = DefaultValue.GetHostIpOrName()
            otherNodes = self.clusterInfo.getClusterNodeNames()
            for otherNode in otherNodes:
                if (otherNode == hostName):
                    otherNodes.remove(otherNode)

            # clean log
            cmd = "rm -rf '%s/%s'; rm -rf /tmp/gauss_*;" % (
                self.clusterInfo.logPath, self.user)
            cmd += "rm -rf '%s/Python-2.7.9'" \
                   % DefaultValue.getClusterToolPath()
            self.logger.debug(
                "Command for deleting logs of other nodes: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "delete user log directory",
                                             self.sshTool,
                                             self.localMode,
                                             self.mpprcFile,
                                             otherNodes)
            self.logger.debug(
                "Successfully deleted logs of the nodes: %s." % otherNodes)
        except Exception as e:
            self.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50207"] % "other nodes log"
                + " Error: \n%s." % str(e))
        self.logger.log("Successfully deleted logs of other nodes.")

    def cleanLocalNodeEnvSoftware(self):
        """
        function: clean local node environment software and variable
        input : NA
        output: NA
        in this function, Gauss-MPPDB* & sctp_patch is came from R5 upgrade R7
        """
        self.logger.log(
            "Deleting software packages "
            "and environmental variables of the local node.")
        try:
            self.clusterToolPath = DefaultValue.getClusterToolPath()

            # clean local node environment software
            path = "%s/%s" % (self.clusterToolPath, PSSHDIR)
            g_file.removeDirectory(path)
            path = "%s/upgrade.sh" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/version.cfg" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/GaussDB.py" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/libcgroup" % self.clusterToolPath
            g_file.removeDirectory(path)
            path = "%s/unixodbc" % self.clusterToolPath
            g_file.removeDirectory(path)
            path = "%s/server.key.cipher" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/server.key.rand" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/%s*" % (self.clusterToolPath, VersionInfo.PRODUCT_NAME)
            g_file.removeDirectory(path)
            path = "%s/server.key.rand" % self.clusterToolPath
            g_file.removeFile(path)
            path = "%s/Gauss*" % (self.clusterToolPath)
            g_file.removeDirectory(path)
            path = "%s/sctp_patch" % (self.clusterToolPath)
            g_file.removeDirectory(path)
            self.logger.debug(
                "Deleting environmental software of local nodes.")

            # clean local node environment variable
            cmd = "(if [ -s '%s' ]; then " % PROFILE_FILE
            cmd += "sed -i -e '/^export GPHOME=%s$/d' %s " % (
                self.clusterToolPath.replace('/', '\/'), PROFILE_FILE)
            cmd += \
                "-e '/^export PATH=\$GPHOME\/pssh-2.3.1\/bin:" \
                "\$GPHOME\/script:\$PATH$/d' %s " % PROFILE_FILE
            cmd += \
                "-e '/^export PATH=\$GPHOME\/script\/gspylib\/pssh\/bin:" \
                "\$GPHOME\/script:\$PATH$/d' %s " % PROFILE_FILE
            cmd += \
                "-e '/^export LD_LIBRARY_PATH=\$GPHOME\/script" \
                "\/gspylib\/clib:\$LD_LIBRARY_PATH$/d' %s " % PROFILE_FILE
            cmd += \
                "-e '/^export LD_LIBRARY_PATH=\$GPHOME\/lib:" \
                "\$LD_LIBRARY_PATH$/d' %s " % PROFILE_FILE
            cmd += \
                "-e '/^export PATH=\/root\/gauss_om\/%s\/script:" \
                "\$PATH$/d' %s " % (self.user, PROFILE_FILE)
            cmd += \
                "-e '/^export PYTHONPATH=\$GPHOME\/lib$/d' %s; fi) " \
                % PROFILE_FILE
            self.logger.debug(
                "Command for deleting environment variable: %s" % cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit(
                    ErrorCode.GAUSS_502["GAUSS_50207"]
                    % "environment variables of the local node"
                    + " Error: \n%s" % output)

            # check if user profile exist
            userProfile = ""
            if (self.mpprcFile is not None and self.mpprcFile != ""):
                userProfile = self.mpprcFile
            else:
                userProfile = "/home/%s/.bashrc" % self.user
            if (not os.path.exists(userProfile)):
                self.logger.debug(
                    "The %s does not exist. "
                    "Please skip to clean $GAUSS_ENV." % userProfile)
                return
            # clean user's environmental variable
            DefaultValue.cleanUserEnvVariable(userProfile,
                                              cleanGAUSS_WARNING_TYPE=True)

            # clean $GAUSS_ENV
            if (not self.deleteUser):
                envContent = "^\\s*export\\s*GAUSS_ENV=.*$"
                g_file.deleteLine(userProfile, envContent)
                self.logger.debug("Command for deleting $GAUSS_ENV: %s" % cmd,
                                  "constant")

        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.log(
            "Successfully deleted software packages "
            "and environmental variables of the local nodes.")

    def cleanLocalOsUser(self):
        """
        function: Clean local os user
        input : NA
        output: NA
        """
        if (not self.deleteUser):
            if (self.localMode):
                cmd = "rm -rf '%s'" % self.clusterInfo.appPath
                DefaultValue.execCommandWithMode(cmd,
                                                 "delete install directory",
                                                 self.sshTool, self.localMode,
                                                 self.mpprcFile)
            return

        group = grp.getgrgid(pwd.getpwnam(self.user).pw_gid).gr_name

        # clean local user
        self.logger.log("Deleting local OS user.")
        cmd = "%s -U %s -l %s" % (
            OMCommand.getLocalScript("Local_Clean_OsUser"), self.user,
            self.localLog)
        self.logger.debug("Command for deleting local OS user: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(output)
        self.logger.log("Successfully deleted local OS user.")

        if (self.deleteGroup):
            # clean local user group
            self.logger.debug("Deleting local OS group.")
            cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
                OMCommand.getLocalScript("Local_UnPreInstall"),
                ACTION_DELETE_GROUP,
                group,
                self.localLog,
                self.xmlFile)
            self.logger.debug("Command for deleting local OS group: %s" % cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.log(output.strip())
            self.logger.debug("Deleting local group is completed.")

    def cleanLocalLog(self):
        """
        function: Clean default log
        input : NA
        output: NA
        """
        self.logger.log("Deleting local node's logs.", "addStep")
        try:
            # clean log
            path = "%s/%s" % (self.clusterInfo.logPath, self.user)
            g_file.removeDirectory(path)
        except Exception as e:
            self.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50207"]
                % "logs" + " Error: \n%s." % str(e))
        self.logger.log("Successfully deleted local node's logs.", "constant")

    def cleanMpprcFile(self):
        """
        function: clean mpprc file if we are using environment seperate
        version.
        input : NA
        output: NA
        """
        self.logger.debug("Clean mpprc file.", "addStep")
        # check if mpprcfile is null
        if (self.mpprcFile != ""):
            baseCmd = "rm -rf '%s'" % self.mpprcFile
            # check if local mode
            if (self.localMode):
                (status, output) = subprocess.getstatusoutput(baseCmd)
                if (status != 0):
                    self.logger.logExit(
                        ErrorCode.GAUSS_502["GAUSS_50207"]
                        % "MPPRC file"
                        + " Command: %s. Error: \n%s" % (baseCmd, output))
            else:
                dbNodeNames = self.clusterInfo.getClusterNodeNames()
                for dbNodeName in dbNodeNames:
                    cmd = "pssh -s -H %s '%s'" % (dbNodeName, baseCmd)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        self.logger.logExit(
                            ErrorCode.GAUSS_502["GAUSS_50207"]
                            % "MPPRC file"
                            + " Command: %s. Error: \n%s" % (cmd, output))
        self.logger.debug("Successfully cleaned mpprc file.", "constant")

    def cleanScript(self):
        """
        clean script directory
        """
        self.logger.debug("Clean script path")
        cmd = "%s -t %s -u %s -Q %s" % (
            OMCommand.getLocalScript("Local_UnPreInstall"),
            ACTION_CLEAN_DEPENDENCY, self.user,
            self.clusterToolPath)
        if self.deleteUser:
            cmd += " -P %s" % self.userHome
        DefaultValue.execCommandWithMode(cmd, "clean script",
                                         self.sshTool, self.localMode,
                                         self.mpprcFile)
        self.logger.debug("Clean script path successfully.")

    def cleanSyslogConfig(self):
        """
        function: clean syslog config
        input : NA
        output: NA
        """
        try:
            # only suse11/suse12 can support it
            distname = g_Platform.dist()[0]
            if (distname.upper() != "SUSE"):
                return

            # clean syslog-ng/rsyslog config
            cmd = "%s -t %s -u %s -l '%s' -X '%s'" % (
                OMCommand.getLocalScript("Local_UnPreInstall"),
                ACTION_CLEAN_SYSLOG_CONFIG,
                self.user,
                self.localLog,
                self.xmlFile)
            self.logger.debug(
                "Command for clean syslog-ng/rsyslog config: %s" % cmd)
            DefaultValue.execCommandWithMode(
                cmd,
                "clean syslog-ng/rsyslog config",
                self.sshTool,
                self.localMode,
                self.mpprcFile,
                self.clusterInfo.getClusterNodeNames())
        except Exception as e:
            self.logger.logExit(str(e))

    def sshExecWithPwd(self, host):
        """
        function: execute command with root password
        input : host
        output: NA
        """
        cmd = "rm -rf %s/* && echo 'OKOKOK'" % gphome
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, 22, "root", self.sshpwd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.read()
        self.logger.debug("%s: %s" % (str(host), str(output)))
        if output.find('OKOKOK') < 0:
            errMsg = stderr.read()
            raise Exception(
                ErrorCode.GAUSS_514["GAUSS_51400"]
                % cmd + "host: %s. Error:\n%s"
                % (host, output))

    def verifyCleanGphome(self, localMode=True):
        """
        function: verify clean gphome and get root password
        input : localMode
        output: str
        """
        sshpwd = ""
        flag = input(
            "Are you sure you want to clean gphome[%s] (yes/no)? " % gphome)
        while (True):
            if (
                    flag.upper() != "YES"
                    and flag.upper() != "NO"
                    and flag.upper() != "Y" and flag.upper() != "N"):
                flag = input("Please type 'yes' or 'no': ")
                continue
            break
        if (flag.upper() == "NO" or flag.upper() == "N"):
            sys.exit(0)
        if "HOST_IP" in os.environ.keys() and not localMode:
            sshpwd = getpass.getpass("Please enter password for root:")
            sshpwd_check = getpass.getpass("Please repeat password for root:")
            if sshpwd_check != sshpwd:
                sshpwd_check = ""
                sshpwd = ""
                raise Exception(ErrorCode.GAUSS_503["GAUSS_50306"] % "root")
            sshpwd_check = ""
        return sshpwd

    def checkAuthentication(self, hostname):
        """
        function: Ensure the proper password-less access to the remote host.
        input : hostname
        output: True/False, hostname
        """
        cmd = 'ssh -n %s %s true' % (DefaultValue.SSH_OPTION, hostname)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.debug("The cmd is %s " % cmd)
            self.logger.debug(
                "Failed to check authentication. Hostname:%s. Error: \n%s" % (
                    hostname, output))
            return (False, hostname)
        return (True, hostname)

    def getItemValueFromXml(self, itemName):
        """
        function: Get item from xml tag CLUSTER.
        input : hostname
        output: True/False, hostname
        """
        (retStatus, retValue) = readOneClusterConfigItem(
            initParserXMLFile(self.xmlFile), itemName, "cluster")
        if (retStatus != 0):
            raise Exception(
                ErrorCode.GAUSS_502["GAUSS_50204"]
                % itemName + " Error: \n%s" % retValue)
        return retValue

    def cleanGphomeScript(self):
        """
        function: clean gphome script
        input : NA
        output: NA
        """
        try:
            if not self.clean_gphome:
                return
            global gphome
            gphome = os.path.normpath(
                self.getItemValueFromXml("gaussdbToolPath"))
            cmd = "rm -rf %s/*" % gphome
            if "HOST_IP" in os.environ.keys():
                # Agent Mode
                if self.localMode:
                    # clean gphome in local mode
                    self.verifyCleanGphome()
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(
                            ErrorCode.GAUSS_514["GAUSS_51400"]
                            % cmd + " Error:\n%s" % output)
                    self.logger.logExit("Successfully clean gphome locally.")
                else:
                    # clean gphome with specified node
                    self.sshpwd = self.verifyCleanGphome(self.localMode)
                    parallelTool.parallelExecute(self.sshExecWithPwd,
                                                 self.clean_host)
                    self.logger.logExit(
                        "Successfully clean gphome on node %s."
                        % self.clean_host)

            else:
                # SSH Mode
                SSH_TRUST = True
                self.nodeList = self.getItemValueFromXml("nodeNames").split(
                    ",")
                if len(self.nodeList) == 0:
                    raise Exception(
                        ErrorCode.GAUSS_502["GAUSS_50203"] % "nodeList")
                results = parallelTool.parallelExecute(
                    self.checkAuthentication, self.nodeList)
                for (key, value) in results:
                    if (not key):
                        self.logger.log("SSH trust has not been created. \
                        \nFor node : %s. Only clean local node." % value,
                                        "constant")
                        SSH_TRUST = False
                        break
                if SSH_TRUST and not self.localMode:
                    # SSH trust has been created
                    self.verifyCleanGphome()
                    parallelTool.parallelExecute(self.sshExecWithPwd,
                                                 self.nodeList)
                if not SSH_TRUST or self.localMode:
                    # SSH trust has not been created
                    # which means clean gphome locally
                    self.verifyCleanGphome()
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(
                            ErrorCode.GAUSS_514["GAUSS_51400"]
                            % cmd + " Error:\n%s" % output)
                self.logger.logExit("Successfully clean gphome.")

        except Exception as e:
            self.logger.logExit(str(e))

    def run(self):
        try:
            self.logger.debug(
                "gs_postuninstall execution takes %s steps in total"
                % ClusterCommand.countTotalSteps("gs_postuninstall"))
            self.cleanGphomeScript()
            self.checkLogFilePath()
            self.cleanSyslogConfig()
            self.doCleanEnvironment()
            self.logger.closeLog()
            self.cleanLocalLog()
            self.cleanMpprcFile()
            self.cleanScript()
            self.logger.log("Successfully cleaned environment.")
        except Exception as e:
            self.logger.logExit(str(e))
        sys.exit(0)
