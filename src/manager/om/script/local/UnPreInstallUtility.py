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
# Description : UnPreInstallUtility.py is a utility to execute unPreInstall.
#############################################################################
import sys
import os
import getopt
import subprocess
import grp

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.VersionInfo import VersionInfo
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsnetwork import g_network
from gspylib.os.gsservice import g_service
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.os.gsfile import g_Platform

ACTION_CLEAN_SYSLOG_CONFIG = 'clean_syslog_config'
ACTION_CLEAN_TOOL_ENV = 'clean_tool_env'
ACTION_CHECK_UNPREINSTALL = "check_unpreinstall"
ACTION_CLEAN_GAUSS_ENV = "clean_gauss_env"
ACTION_DELETE_GROUP = "delete_group"
# clean instance paths
ACTION_CLEAN_INSTANCE_PATHS = "clean_instance_paths"
# clean $GAUSS_ENV
ACTION_CLEAN_ENV = "clean_env"
# clean dependency directory
ACTION_CLEAN_DEPENDENCY = "clean_dependency"

PROFILE_FILE = '/etc/profile'
PSSHDIR = 'pssh-2.3.1'
LIBPATH = "lib"
SCRIPTPATH = "script"
#####################################################
# syslog variables
#####################################################
RSYSLOG = "rsyslog"
RSYSLOG_CONFIG_FILE = "/etc/rsyslog.conf"
RSYSLOG_FACILITY_LEVEL = "local3.*"
AP_RSYSLOG_FACILITY_LEVEL = ":msg,contains,\"MPPDB\""
SYSLOG_NG = "syslog-ng"
SYSLOG_NG_CONFIG_FILE = "/etc/syslog-ng/syslog-ng.conf"
SYSLOG_NG_CONFIG_FILE_SERVER = "/etc/sysconfig/syslog"

g_nodeInfo = None


class Postuninstall(LocalBaseOM):
    """
    execute unPreInstall
    """

    def __init__(self):
        self.action = ""
        self.userInfo = ""
        self.user = ""
        self.group = ""
        self.clusterConfig = ""
        self.preparePath = ""
        self.checkEmpty = False
        self.envParams = []
        self.userProfile = ""
        self.logFile = ""
        self.clusterToolPath = ""
        self.tmpFile = ""
        self.component = []
        self.clusterComponent = []
        self.logger = None
        self.userHome = ""

    def initGlobals(self):
        """
        init global variables
        input : NA
        output: NA
        """
        global g_nodeInfo
        self.logger = GaussLog(self.logFile, self.action)

        if self.clusterConfig != "":
            if os.path.isfile(self.clusterConfig):
                self.clusterToolPath = DefaultValue.getPreClusterToolPath(
                    self.user, self.clusterConfig)
                self.readConfigInfoByXML()
                hostName = DefaultValue.GetHostIpOrName()
                g_nodeInfo = self.clusterInfo.getDbNodeByName(hostName)
                if (g_nodeInfo is None):
                    self.logger.logExit(
                        ErrorCode.GAUSS_516["GAUSS_51620"] % "local"
                        + " There is no host named %s!" % hostName)
            else:
                self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50210"] % (
                        "config file [%s]" % self.clusterConfig))

        elif self.action != ACTION_CLEAN_DEPENDENCY:
            try:
                self.clusterToolPath = DefaultValue.getClusterToolPath()
            except Exception as e:
                self.logger.logExit(
                    ErrorCode.GAUSS_502["GAUSS_50219"] %
                    "the cluster tool path" + " Error: \n%s" % str(e))

        if not self.clusterToolPath:
            self.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50219"] % "cluster tool path")

        # make sure if we are using env seperate version,
        # and get the right profile
        # we can not check mppenvfile exists here
        mppenvFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
        if (mppenvFile != "" and mppenvFile is not None):
            self.userProfile = mppenvFile
        else:
            self.userProfile = "/home/%s/.bashrc" % self.user

    def usage(self):
        """
    Usage:
      python3 UnPreInstallUtility.py -t action -u user [-X xmlfile] [-l log]
    Common options:
      -t                                the type of action
      -u                                the os user of cluster
      -X                                the xml file path
      -l                                the path of log file
      --help                            show this help, then exit
        """
        print(self.usage.__doc__)

    def parseCommandLine(self):
        """
        function: Check parameter from command line
        input : NA
        output: NA
        """
        try:
            opts, args = getopt.getopt(
                sys.argv[1:], "t:u:X:l:f:Q:P:", ["help"])
        except Exception as e:
            self.usage()
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

        if (len(args) > 0):
            GaussLog.exitWithError(
                ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

        for (key, value) in opts:
            if (key == "--help"):
                self.usage()
                sys.exit(0)
            elif (key == "-t"):
                self.action = value
            elif (key == "-u"):
                self.user = value
            elif (key == "-X"):
                self.clusterConfig = value
            elif (key == "-l"):
                self.logFile = os.path.realpath(value)
            elif (key == "-f"):
                self.tmpFile = value
            elif key == "-Q":
                self.clusterToolPath = value
            elif key == "-P":
                self.userHome = value
            else:
                GaussLog.exitWithError(
                    ErrorCode.GAUSS_500["GAUSS_50000"] % key)
            Parameter.checkParaVaild(key, value)

    def checkParameter(self):
        """
        function: Check parameter from command line
        input : NA
        output: NA
        """

        if self.action == "":
            GaussLog.exitWithError(
                ErrorCode.GAUSS_500["GAUSS_50001"] % "t" + ".")

        if self.logFile == "":
            self.logFile = DefaultValue.getOMLogPath(
                DefaultValue.LOCAL_LOG_FILE, self.user, "")

        if self.user == "" and self.action != ACTION_CLEAN_DEPENDENCY:
            GaussLog.exitWithError(
                ErrorCode.GAUSS_500["GAUSS_50001"] % "u" + ".")

    def getSyslogType(self):
        """
        function: judge syslog type
        input : NA
        output: str
        """
        self.logger.debug("Judging the syslog type is rsyslog or syslog-ng.")
        if (os.path.isfile(RSYSLOG_CONFIG_FILE)):
            return RSYSLOG
        elif (os.path.isfile(SYSLOG_NG_CONFIG_FILE)):
            return SYSLOG_NG
        else:
            self.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50219"] % "rsyslog or syslog-ng" +
                " \nError: Failed to judge the syslog type.")

    def cleanWarningConfig(self):
        """
        function: clean syslog-ng/rsyslog config
        input : NA
        output: NA
        """
        self.logger.debug("Cleaning syslog-ng configuration.")
        # judge the installed syslog type on the local host is rsyslog
        # or syslog-ng
        syslogType = self.getSyslogType()
        if (syslogType == SYSLOG_NG):
            self.cleanWarningConfigForSyslogng()
        elif (syslogType == RSYSLOG):
            self.cleanWarningConfigForRsyslog()
        self.logger.debug("Successfully cleaned system log.")

    def cleanWarningConfigForSyslogng(self):
        """
        function: clean syslog-ng config
        input : NA
        output: NA
        """
        # clean client syslog-ng configure
        cmd = "(if [ -s '%s' ]; then " % SYSLOG_NG_CONFIG_FILE
        cmd += \
            "sed -i -e '/^filter f_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += "-e '/^destination d_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += \
            "-e '/^log { source(src); filter(f_gaussdb); " \
            "destination(d_gaussdb); };$/d' %s;fi;) " % SYSLOG_NG_CONFIG_FILE
        self.logger.debug("Command for cleaning client system log: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                + " Error:\n%s" % output)

        # clean server syslog-ng configure
        cmd = "(if [ -s '%s' ]; then " % SYSLOG_NG_CONFIG_FILE
        cmd += \
            "sed -i -e '/^template t_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += "-e '/^source s_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += "-e '/^filter f_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += "-e '/^destination d_gaussdb.*$/d' %s " % SYSLOG_NG_CONFIG_FILE
        cmd += \
            "-e '/^log { source(s_gaussdb); " \
            "filter(f_gaussdb); destination(d_gaussdb); };$/d' %s;" \
            "fi; " % SYSLOG_NG_CONFIG_FILE
        cmd += "if [ -s '%s' ]; then " % SYSLOG_NG_CONFIG_FILE_SERVER
        cmd += \
            "sed -i -e '/^SYSLOGD_OPTIONS=\\\"-r -m 0\\\"/d' %s " \
            % SYSLOG_NG_CONFIG_FILE_SERVER
        cmd += "-e '/^KLOGD_OPTIONS=\\\"-x\\\"/d' %s; " \
               % SYSLOG_NG_CONFIG_FILE_SERVER
        cmd += "fi) "
        self.logger.debug("Command for cleaning server system log: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                " Error:\n%s" % output)

        # restart the syslog service
        (status, output) = g_service.manageOSService("syslog", "restart")
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_508["GAUSS_50802"] % "restart syslog"
                + " Error: \n%s" % output)

    def cleanWarningConfigForRsyslog(self):
        """
        function: clean rsyslog config
        input : NA
        output: NA
        """
        # clean rsyslog config on client and server
        cmd = "(if [ -s '%s' ]; then " % RSYSLOG_CONFIG_FILE
        cmd += \
            "sed -i -e '/^$ModLoad imjournal.*$/d' %s " % RSYSLOG_CONFIG_FILE
        cmd += "-e '/^$ModLoad imudp.*$/d' %s " % RSYSLOG_CONFIG_FILE
        cmd += "-e '/^$UDPServerRun 514.*$/d' %s " % RSYSLOG_CONFIG_FILE
        cmd += \
            "-e '/^$imjournalRatelimitInterval.*$/d' %s " % RSYSLOG_CONFIG_FILE
        cmd += "-e '/^$imjournalRatelimitBurst.*$/d' %s " % RSYSLOG_CONFIG_FILE
        cmd += "-e '/^%s.*$/d' %s; " % (
            AP_RSYSLOG_FACILITY_LEVEL, RSYSLOG_CONFIG_FILE)
        cmd += "fi) "
        self.logger.debug("Command for cleaning crash rsyslog: %s." % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50207"] % 'crash rsyslog'
                + " Error: \n%s" % output)

        # restart the rsyslog service
        (status, output) = g_service.manageOSService("rsyslog", "restart")
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_508["GAUSS_50802"] % "restart rsyslog"
                + " Error: \n%s" % output)

    def cleanEnvSoftware(self):
        """
        function: clean environment software and variable
        Gauss-MPPDB* & sctp_patch is came from R5 upgrade R7
        input : NA
        output: NA
        """
        self.logger.debug("Cleaning the environmental software and variable.")
        # clean environment software
        path = "%s/%s" % (self.clusterToolPath, PSSHDIR)
        g_file.removeDirectory(path)
        path = "%s/lib" % self.clusterToolPath
        g_file.removeDirectory(path)
        path = "%s/script" % self.clusterToolPath
        g_file.removeDirectory(path)
        path = "%s/sudo" % self.clusterToolPath
        g_file.removeDirectory(path)
        path = "%s/upgrade.sh" % self.clusterToolPath
        g_file.removeFile(path)
        path = "%s/version.cfg" % self.clusterToolPath
        g_file.removeFile(path)
        path = "%s/GaussDB.py" % self.clusterToolPath
        g_file.removeFile(path)
        path = "%s/libcgroup" % self.clusterToolPath
        g_file.removeDirectory(path)
        path = "%s/server.key.cipher" % self.clusterToolPath
        g_file.removeFile(path)
        path = "%s/server.key.rand" % self.clusterToolPath
        g_file.removeFile(path)
        path = "%s/%s*" % (self.clusterToolPath, VersionInfo.PRODUCT_NAME)
        g_file.removeDirectory(path)
        path = "%s/Gauss*" % (self.clusterToolPath)
        g_file.removeDirectory(path)
        path = "%s/sctp_patch" % (self.clusterToolPath)
        g_file.removeDirectory(path)
        path = "%s/unixodbc" % self.clusterToolPath
        g_file.removeDirectory(path)
        self.logger.debug(
            "Successfully cleaned the environmental software and variable.")

        self.logger.debug("Cleaning environmental software.")
        # clean environment variable
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
            "-e '/^export LD_LIBRARY_PATH=\$GPHOME\/lib:" \
            "\$LD_LIBRARY_PATH$/d' %s " % PROFILE_FILE
        cmd += \
            "-e '/^export PYTHONPATH=\$GPHOME\/lib$/d' %s; fi) " % PROFILE_FILE
        self.logger.debug(
            "Command for cleaning environment variable: %s." % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                + " Error:\n%s" % output)

        self.logger.debug(
            "Successfully cleaned environmental software and variable.")

    def checkUnPreInstall(self):
        """
        function: check whether do uninstall before unpreinstall
        input : NA
        output: NA
        """
        self.logger.debug("Checking UnPreInstall.")
        # check if user exist
        try:
            DefaultValue.getUserId(self.user)
        except Exception as e:
            self.logger.logExit(str(e))

        # check if user profile exist
        if (not os.path.exists(self.userProfile)):
            self.logger.debug(
                "The %s does not exist." % self.userProfile
                + " Please skip to check UnPreInstall.")
            return

            # check $GAUSSHOME
        cmd = "su - %s -c 'source %s && echo $GAUSS_ENV' 2>/dev/null" % (
            self.user, self.userProfile)
        self.logger.debug("Command for getting $GAUSSHOME: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                + " Error:\n%s" % output)
        gaussEnv = output.strip()
        if (gaussEnv == "2"):
            self.logger.logExit(
                ErrorCode.GAUSS_525["GAUSS_52501"] % "gs_uninstall")

            # check $GAUSS_ENV
        cmd = "su - %s -c 'source %s && echo $GAUSS_ENV' 2>/dev/null" % (
            self.user, self.userProfile)
        self.logger.debug("Command for getting $GAUSS_ENV: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                + " Error:\n%s" % output)
        gaussEnv = output.strip()

        if (str(gaussEnv) != "1"):
            self.logger.logExit(
                ErrorCode.GAUSS_525["GAUSS_52501"] % "gs_preinstall")

        self.logger.debug("Successfully checked UnPreInstall.")

    def cleanGaussEnv(self):
        """
        function: clean $GAUSS_ENV
        input : NA
        output: NA
        """
        self.logger.debug("Cleaning $GAUSS_ENV.")

        # check if user profile exist
        if (self.userProfile is not None and self.userProfile != ""):
            userProfile = self.userProfile
        else:
            userProfile = "/home/%s/.bashrc" % self.user

        if (not os.path.exists(userProfile)):
            self.logger.debug(
                "The %s does not exist." % userProfile
                + " Please skip to clean $GAUSS_ENV.")
            return
        # clean user's environmental variable
        DefaultValue.cleanUserEnvVariable(userProfile,
                                          cleanGAUSS_WARNING_TYPE=True)

        # clean $GAUSS_ENV
        envContent = "^\\s*export\\s*GAUSS_ENV=.*$"
        g_file.deleteLine(userProfile, envContent)

        self.logger.debug("Cleaned $GAUSS_ENV.")

    def cleanNetworkfile(self, backIpNIC, virtualIp):
        """
        function: clean configured IP in Network file
        input : NA
        output: NA
        """
        self.logger.debug("Cleaning network file.")
        try:
            # read information from networkfile
            networkfile = "/etc/sysconfig/network/ifcfg-" + backIpNIC
            networkinfo = []
            # check if the file is a link
            g_OSlib.checkLink(networkfile)
            with open(networkfile, "r") as fp:
                networkinfo = fp.readlines()
            LABEL = self.getLABEL(virtualIp, networkfile)
            if (LABEL is not None):
                # init linenum for delete
                del_1 = 0
                del_2 = 0
                linenum = 1
                for line in networkinfo:
                    if (line.split("=")[1].strip() == virtualIp):
                        # find if the netmask exist, if exist, delete this line
                        cmd_g = "grep -n 'NETMASK_%s=' %s" % (
                            LABEL, networkfile)
                        (status, output) = subprocess.getstatusoutput(cmd_g)
                        if (status == 0):
                            linenum_net = int(output.split(":")[0])
                        if (linenum + 1 == linenum_net):
                            del_1 = linenum_net
                        # find if the LABEL number exist,
                        # if exist, delete this line
                        cmd_g = "grep -n 'LABEL_%s=' %s " % (
                            LABEL, networkfile)
                        (status, output) = subprocess.getstatusoutput(cmd_g)
                        if (status == 0):
                            linenum_net = int(output.split(":")[0])
                        if (linenum + 2 == linenum_net):
                            del_2 = linenum_net
                        # delete issues which exist
                        if (del_1 != 0 and del_2 != 0):
                            cmd = "sed -i '%dd;%dd;%dd' %s" % (
                                linenum, del_1, del_2, networkfile)
                        elif (del_1 != 0 and del_2 == 0):
                            cmd = "sed -i '%dd;%dd' %s" % (
                                linenum, del_1, networkfile)
                        elif (del_1 == 0 and del_2 != 0):
                            cmd = "sed -i '%dd;%dd' %s" % (
                                linenum, del_2, networkfile)
                        else:
                            cmd = "sed -i '%dd' %s" % (linenum, networkfile)
                        (status, output) = subprocess.getstatusoutput(cmd)
                        if (status != 0):
                            raise Exception(
                                ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error:\n%s" % output)
                    linenum += 1
                self.logger.log(
                    "Successfully clean virtual Ip from network file")
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] % (
                        "the LABEL number of %s " % virtualIp))
            self.logger.debug("Successfully cleaned network file.")
        except Exception as e:
            self.logger.log("Error: Write networkfile failed." + str(e))

    def IsSuSE12SP0(self):
        """
        function:Check is OS SuSE12.0
        input   :NA
        output  :bool
        """
        if (os.path.isfile("/etc/SuSE-release")):
            cmd = "grep -i 'PATCHLEVEL' /etc/SuSE-release  " \
                  "| awk -F '=' '{print $2}'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0 and output != ""):
                if (output.strip().isdigit() and int(output.strip()) == 0):
                    return True
            else:
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + " Error: \n%s " % output)
        return False

    def getLABEL(self, virtualIp, networkfile):
        """
        function: get LABEL number of virtual ip from network file
        input : fp, virtualIp
        output: int
        """
        # check if the file is a link
        g_OSlib.checkLink(networkfile)
        with open(networkfile, "r") as fp:
            for line in fp:
                if line.split("=")[1].strip() == virtualIp:
                    if line.split("IPADDR_")[1].split("=%s" % virtualIp)[0]:
                        return line.split("IPADDR_")[1].split(
                            "=%s" % virtualIp)[0]
                    else:
                        return None
        return None

    def cleanGroup(self):
        """
        function: clean group
        input : NA
        output: NA
        """
        self.logger.debug("Cleaning user group.")
        hostName = DefaultValue.GetHostIpOrName()
        groupname = self.user

        try:
            groupid = grp.getgrnam(groupname).gr_gid
        except Exception:
            self.logger.debug("group %s has been deleted." % groupname)
            sys.exit(0)

        cmd = "cat /etc/passwd | awk -F [:] '{print $1  \" \"$4}'" \
              "|grep ' %s$'" % groupid
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            self.logger.logExit(
                "Warning: There are other users in the group %s on %s,"
                " skip to delete group." % (groupname, hostName))
        elif status == 1:
            cmd = "groupdel %s" % groupname
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.logger.logExit(
                    "Warning: Failed to delete group "
                    "%s by cmd:%s. Error: \n%s" % (groupname, cmd, output))
        else:
            self.logger.logExit(
                "Warning: Failed to delete group "
                "%s by cmd:%s. Error: \n%s" % (groupname, cmd, output))
        self.logger.debug("Successfully cleaned user group.")

    def cleanScript(self):
        """
        function: clean script
        """
        # clean lib
        libPath = os.path.join(self.clusterToolPath, LIBPATH)
        if os.path.exists(libPath):
            g_file.removeDirectory(libPath)

        # clean om script
        scriptPath = os.path.join(self.clusterToolPath, SCRIPTPATH)
        if os.path.exists(scriptPath):
            g_file.removeDirectory(scriptPath)

        # clean root script path
        root_script_path = os.path.join(DefaultValue.ROOT_SCRIPTS_PATH,
                                        self.user)
        if os.path.exists(root_script_path):
            g_file.removeDirectory(root_script_path)
        # if /root/gauss_om has no files, delete it.
        if not os.listdir(DefaultValue.ROOT_SCRIPTS_PATH):
            g_file.removeDirectory(DefaultValue.ROOT_SCRIPTS_PATH)

        # clean others
        if os.path.exists(self.clusterToolPath):
            g_file.cleanDirectoryContent(self.clusterToolPath)

        if self.userHome != "":
            if os.path.exists(self.userHome):
                g_file.removeDirectory(self.userHome)

    def cleanEnv(self):
        """
        function: clean envriment variable
        """
        self.logger.debug("Begin clean envrionment variable")
        if not self.userProfile:
            self.logger.logExit("Clean Env failed: can not get user profile.")
        for comp in self.clusterComponent:
            comp.cleanEnv(self.userProfile)

        # clean user's environment variable
        self.logger.debug("Clean user environment variable.")
        DefaultValue.cleanUserEnvVariable(self.userProfile,
                                          cleanGAUSS_WARNING_TYPE=True)
        # clean GAUSS_ENV
        self.logger.debug("Clean GAUSS_ENV.")
        g_file.deleteLine(self.userProfile, "^\\s*export\\s*GAUSS_ENV=.*$")
        self.logger.debug("Clean envrionment variable successfully.")

    def cleanPath(self):
        """
        function: clean path
        input: NA
        output: NA
        """
        self.logger.debug("Begin clean path")
        if os.path.exists(self.clusterInfo.appPath):
            self.logger.debug("Deleting the install directory.")
            cleanPath = os.path.join(self.clusterInfo.appPath, "./*")
            g_file.removeDirectory(cleanPath)
            self.logger.debug("Successfully deleted the install directory.")
        for i in self.component:
            i.cleanPath()
        gsdbHomePath = "/home/%s/gsdb_home" % self.user
        if os.path.exists(gsdbHomePath):
            self.logger.debug("Deleting the gsdb home path.")
            g_file.removeDirectory(gsdbHomePath)
            self.logger.debug("Successfully deleted the gsdb home path.")
        self.logger.debug("Clean Path successfully.")

    def run(self):
        try:
            self.parseCommandLine()
            self.checkParameter()
            self.initGlobals()
        except Exception as e:
            GaussLog.exitWithError(str(e))

        try:
            if (self.action == ACTION_CLEAN_SYSLOG_CONFIG):
                self.cleanWarningConfig()
            elif (self.action == ACTION_CLEAN_TOOL_ENV):
                self.cleanEnvSoftware()
            elif (self.action == ACTION_CHECK_UNPREINSTALL):
                self.checkUnPreInstall()
            elif (self.action == ACTION_CLEAN_GAUSS_ENV):
                self.cleanGaussEnv()
            elif (self.action == ACTION_DELETE_GROUP):
                self.cleanGroup()
            elif (self.action == ACTION_CLEAN_DEPENDENCY):
                self.cleanScript()
            elif (self.action == ACTION_CLEAN_ENV):
                self.cleanEnv()
            elif (self.action == ACTION_CLEAN_INSTANCE_PATHS):
                self.cleanPath()
            else:
                self.logger.logExit(
                    ErrorCode.GAUSS_500["GAUSS_50000"] % self.action)
        except Exception as e:
            self.logger.logExit(str(e))


if __name__ == '__main__':
    """
    main function
    """
    try:
        postUninstallUtility = Postuninstall()
        postUninstallUtility.run()
    except Exception as e:
        GaussLog.exitWithError(str(e))
    sys.exit(0)
