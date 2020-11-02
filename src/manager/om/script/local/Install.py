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
# Description  : Install.py is a utility to do gs_install.
#############################################################################

import getopt
import os
import sys
import subprocess
import traceback

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.os.gsfile import g_file
from gspylib.os.gsOSlib import g_OSlib
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.VersionInfo import VersionInfo

#################################################################
ACTION_INSTALL_CLUSTER = "install_cluster"
ACTION_INIT_INSTNACE = "init_instance"
ACTION_CONFIG_CLUSTER = "config_cluster"
ACTION_START_CLUSTER = "start_cluster"
ACTION_CLEAN_TEMP_FILE = "clean_temp_file"
ACTION_PREPARE_CONFIG_CLUSTER = "prepare_config_cluster"
ACTION_BUILD_STANDBY = "build_standby"
ACTION_BUILD_CASCADESTANDBY = "build_cascadestandby"
#################################################################
g_opts = None
g_timer = None


#################################################################

class CmdOptions():
    """
    class: cmdOptions
    """

    def __init__(self):
        """
        Constructor
        """
        self.action = ""
        self.installPath = ""
        self.logPath = ""
        self.tmpPath = ""
        self.user = ""
        self.group = ""
        self.clusterName = ""
        self.clusterConfig = ""
        self.mpprcFile = ""
        self.static_config_file = ""
        self.installflag = False
        self.logFile = ""
        self.alarmComponent = ""
        self.dws_mode = False
        self.upgrade = False
        self.productVersion = None
        # License mode
        self.licenseMode = None
        self.time_out = None
        self.logger = None


def usage():
    """
Usage:
  python3 --help | -?
  python3 Install.py -t action -U username:groupname -X xmlfile
  [--alarm=ALARMCOMPONENT]
  [-l logfile]
  [--dws-mode]
  [-R installPath]
  [-c clusterName]
  [-M logPath]
  [-P tmpPath]
  [-f staticConfigFile]
Common options:
  -t                                The type of action.
  -U                                The user and group name.
  -X --xmlfile = xmlfile            Cluster config file.
     --alarm = ALARMCOMPONENT       alarm component path.
     --dws-mode                     dws mode.
  -l --log-file=logfile             The path of log file.
  -R                                Install path.
  -c                                Cluster name.
  -M                                The directory of log file.
  -P                                The tmp path.
  -f                                The static_config_file.
  -? --help                         Show this help screen.
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: parse input parameters
    input : NA
    output: NA
    """
    try:
        # option '-M' specify the environment parameter GAUSSLOG
        # option '-P' specify the environment parameter PGHOST|GAUSSTMP
        # option '-u' install new binary for upgrade
        opts, args = getopt.getopt(sys.argv[1:], "t:U:X:R:M:P:i:l:c:f:Tu",
                                   ["alarm=", "dws-mode", "time_out=",
                                    "product=", "licensemode="])
    except getopt.GetoptError as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(e))

    if len(args) > 0:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    parameter_map = {"-X": g_opts.clusterConfig, "-R": g_opts.installPath,
                     "-l": g_opts.logFile, "-c": g_opts.clusterName,
                     "-M": g_opts.logPath, "-P": g_opts.tmpPath,
                     "-f": g_opts.static_config_file,
                     "--alarm": g_opts.alarmComponent,
                     "--licensemode": g_opts.licenseMode,
                     "--time_out": g_opts.time_out}
    parameter_keys = parameter_map.keys()
    for key, value in opts:
        if key == "-U":
            strTemp = value
            strList = strTemp.split(":")
            if len(strList) != 2:
                GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"]
                                       % "U")
            if strList[0] == "" or strList[1] == "":
                GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"]
                                       % "U")
            g_opts.user = strList[0]
            g_opts.group = strList[1]
        elif key in parameter_keys:
            parameter_map[key] = value
        elif key == "-t":
            g_opts.action = value
        elif key == "--dws-mode":
            g_opts.dws_mode = True
        elif key == "-u":
            g_opts.upgrade = True
        elif key == "-T":
            g_opts.installflag = True
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % value)
        Parameter.checkParaVaild(key, value)

    g_opts.clusterConfig = parameter_map["-X"]
    g_opts.installPath = parameter_map["-R"]
    g_opts.logFile = parameter_map["-l"]
    g_opts.clusterName = parameter_map["-c"]
    g_opts.logPath = parameter_map["-M"]
    g_opts.tmpPath = parameter_map["-P"]
    g_opts.static_config_file = parameter_map["-f"]
    g_opts.alarmComponent = parameter_map["--alarm"]
    g_opts.licenseMode = parameter_map["--licensemode"]
    g_opts.time_out = parameter_map["--time_out"]


def checkParameterEmpty(parameter, parameterName):
    """
    function: check parameter empty
    input : parameter, parameterName
    output: NA
    """
    if parameter == "":
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                               % parameterName + ".")


def checkParameter():
    """
    function: check install parameter
    input : NA
    output: NA
    """
    if g_opts.action == "":
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 't' + '.')

    if (g_opts.action != ACTION_INSTALL_CLUSTER
            and g_opts.action != ACTION_PREPARE_CONFIG_CLUSTER
            and g_opts.action != ACTION_INIT_INSTNACE
            and g_opts.action != ACTION_CONFIG_CLUSTER
            and g_opts.action != ACTION_START_CLUSTER
            and g_opts.action != ACTION_CLEAN_TEMP_FILE
            and g_opts.action != ACTION_BUILD_STANDBY
            and g_opts.action != ACTION_BUILD_CASCADESTANDBY):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"] % "t")

    if (g_opts.clusterConfig != "" and
            not os.path.exists(g_opts.clusterConfig)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50201"]
                               % g_opts.clusterConfig)

    if (g_opts.logPath != "" and not os.path.exists(g_opts.logPath)
            and not os.path.isabs(g_opts.logPath)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50219"]
                               % g_opts.logPath)

    if (g_opts.static_config_file != "" and
            not os.path.isfile(g_opts.static_config_file)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50219"]
                               % g_opts.static_config_file)

    # check mpprc file path
    g_opts.mpprcFile = DefaultValue.getMpprcFile()
    g_opts.logger = GaussLog(g_opts.logFile)
    checkParameterEmpty(g_opts.user, "U")
    g_opts.installPath = os.path.normpath(g_opts.installPath)
    g_opts.installPath = os.path.realpath(g_opts.installPath)
    g_opts.logger.log("Using " + g_opts.user + ":" + g_opts.group
                      + " to install database.")
    g_opts.logger.log("Using installation program path : "
                      + g_opts.installPath)

    if g_opts.logFile == "":
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, g_opts.user, "",
            g_opts.clusterConfig)

    if g_opts.alarmComponent == "":
        g_opts.alarmComponent = DefaultValue.ALARM_COMPONENT_PATH


def createLinkToApp():
    """
    function: create link to app
    input  : NA
    output : NA
    """
    if g_opts.upgrade:
        g_opts.logger.log("Under upgrade process,"
                          " no need to create symbolic link.")
        return
    g_opts.logger.debug("Created symbolic link to $GAUSSHOME with commitid.")
    gaussHome = DefaultValue.getInstallDir(g_opts.user)
    if gaussHome == "":
        raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$GAUSSHOME")
    versionFile = VersionInfo.get_version_file()
    commitid = VersionInfo.get_version_info(versionFile)[2]
    actualPath = gaussHome + "_" + commitid
    if os.path.exists(gaussHome):
        if not os.path.islink(gaussHome):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50200"] % gaussHome
                            + " Cannot create symbolic link,"
                              " please rename or delete it.")
        else:
            if os.path.realpath(gaussHome) == actualPath:
                g_opts.logger.log("$GAUSSHOME points to %s, no need to create"
                                  " symbolic link." % actualPath)
                return

    cmd = "ln -snf %s %s" % (actualPath, gaussHome)
    g_opts.logger.log("Command for creating symbolic link: %s." % cmd)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        g_opts.logger.log(output)
        g_opts.logger.logExit(ErrorCode.GAUSS_501["GAUSS_50107"] % "app.")
    g_opts.logger.debug("Successfully created symbolic link to"
                        " $GAUSSHOME with commitid.")


class Install(LocalBaseOM):
    """
    class: install
    """

    def __init__(self, logFile, user, clusterConf, dwsMode=False,
                 mpprcFile="", installPath="", alarmComponent="",
                 upgrade=False):
        """
        function: Constructor
        input : logFile, user, clusterConf, dwsMode, mpprcFile, installPath
                alarmComponent, upgrade
        output: NA
        """
        LocalBaseOM.__init__(self, logFile, user, clusterConf, dwsMode)

        if self.clusterConfig == "":
            # Read config from static config file
            self.readConfigInfo()
        else:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromXml(self.clusterConfig,
                                         g_opts.static_config_file)
            hostName = DefaultValue.GetHostIpOrName()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if self.dbNodeInfo is None:
                self.logger.logExit(ErrorCode.GAUSS_516["GAUSS_51619"]
                                    % hostName)
        # get user info
        self.getUserInfo()
        if user != "" and self.user != user.strip():
            self.logger.debug("User parameter : %s." % user)
            self.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50315"]
                                % (self.user, self.clusterInfo.appPath))
        # init every component
        self.initComponent()

        self.mpprcFile = mpprcFile
        self.installPath = installPath
        self.alarmComponent = alarmComponent
        self.upgrade = upgrade
        # This script will be not validating the parameters.
        # Because this should be detected by which instance call
        #  this local script.
        self.productVersion = None
        self.time_out = None

    def __decompressBinPackage(self):
        """
        function: Install database binary file.
        input : NA
        output: NA
        """
        if self.dws_mode:
            self.logger.log("Copying bin file.")
            bin_image_path = DefaultValue.DWS_APP_PAHT
            srcPath = "'%s'/*" % bin_image_path
            destPath = "'%s'/" % self.installPath
            cmd = g_file.SHELL_CMD_DICT["copyFile"] % (srcPath, destPath)
            self.logger.debug("Copy command: " + cmd)
            status, output = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50214"]
                                    % srcPath + " Error: " + output)
        else:
            self.logger.log("Decompressing bin file.")
            tarFile = g_OSlib.getBz2FilePath()
            # let bin executable
            g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, tarFile)

            cmd = "export LD_LIBRARY_PATH=$GPHOME/script/gspylib/clib:" \
                  "$LD_LIBRARY_PATH && "
            # decompress tar file.
            strCmd = cmd + "tar -xpf \"" + tarFile + "\" -C \"" + \
                     self.installPath + "\""
            self.logger.log("Decompress command: " + strCmd)
            status, output = subprocess.getstatusoutput(strCmd)
            if status != 0:
                self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50217"]
                                    % tarFile + " Error: \n%s" % str(output))

            # change owner for tar file.
            g_file.changeOwner(self.user, self.installPath, True)
        self.logger.log("Successfully decompressed bin file.")

    def __saveUpgradeVerionInfo(self):
        """
        function: save upgrade version info
        input: NA
        output: NA
        """
        if self.dws_mode:
            versionCfgFile = "%s/version.cfg" % DefaultValue.DWS_PACKAGE_PATH
            upgradeVersionFile = "%s/bin/upgrade_version" % self.installPath
        else:
            dirName = os.path.dirname(os.path.realpath(__file__))
            versionCfgFile = "%s/../../version.cfg" % dirName
            upgradeVersionFile = "%s/bin/upgrade_version" % self.installPath

        if not os.path.exists(versionCfgFile):
            self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"]
                                % versionCfgFile)
        if not os.path.isfile(versionCfgFile):
            self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50210"]
                                % versionCfgFile)

        try:
            # read version info from version.cfg file
            (newClusterVersion, newClusterNumber, commitId) = \
                VersionInfo.get_version_info(versionCfgFile)
            # save version info to upgrade_version file
            if os.path.isfile(upgradeVersionFile):
                os.remove(upgradeVersionFile)

            g_file.createFile(upgradeVersionFile)
            g_file.writeFile(upgradeVersionFile,
                             [newClusterVersion, newClusterNumber, commitId])
            g_file.changeMode(DefaultValue.KEY_FILE_MODE, upgradeVersionFile)
        except Exception as e:
            self.logger.logExit(str(e))

    def __modifyAlarmItemConfFile(self):
        """
        function: modify alarm item conf file
        input: NA
        output: NA
        """
        # modify alarmItem.conf file
        alarmItemConfigFile = "%s/bin/alarmItem.conf" % self.installPath
        if not os.path.exists(alarmItemConfigFile):
            self.logger.log("Alarm's configuration file %s does not exist."
                            % alarmItemConfigFile)
            return

        self.logger.log("Modifying Alarm configuration.")
        g_file.replaceFileLineContent("^.*\(alarm_component.*=.*\)", "#\\1",
                                      alarmItemConfigFile)
        g_file.writeFile(alarmItemConfigFile, ['    '])
        g_file.writeFile(alarmItemConfigFile, ['alarm_component = %s'
                                               % self.alarmComponent])

    def __createStaticConfig(self):
        """
        function: Save cluster info to static config
        input : NA
        output: NA
        """
        staticConfigPath = "%s/bin/cluster_static_config" % self.installPath
        # save static config
        nodeId = self.dbNodeInfo.id
        self.clusterInfo.saveToStaticConfig(staticConfigPath, nodeId)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE, staticConfigPath)
        g_file.changeOwner(self.user, staticConfigPath, False)

    def __bakInstallPackage(self):
        """
        function: backup install package for replace
        input : NA
        output: NA
        """
        dirName = os.path.dirname(os.path.realpath(__file__))
        packageFile = "%s/%s" % (os.path.join(dirName, "./../../"),
                                 DefaultValue.get_package_back_name())
        # Check if MPPDB package exist
        if not os.path.exists(packageFile):
            self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"]
                                % 'MPPDB package' + " Can not back up.")
        # Save MPPDB package to bin path
        destPath = "'%s'/bin/" % self.installPath
        g_file.cpFile(packageFile, destPath)

    def __fixInstallPathPermission(self):
        """
        function: fix the whole install path's permission
        input : NA
        output: NA
        """
        installPathFileTypeDict = {}
        try:
            # get files type
            installPathFileTypeDict = g_file.getFilesType(self.installPath)
        except Exception as e:
            self.logger.logExit(str(e))

        for key in installPathFileTypeDict:
            if not os.path.exists(key):
                self.logger.debug("[%s] does not exist. Please skip it."
                                  % key)
                continue
            if os.path.islink(key):
                self.logger.debug("[%s] is a link file. Please skip it."
                                  % key)
                continue
            # skip DbClusterInfo.pyc
            if os.path.basename(key) == "DbClusterInfo.pyc":
                continue
            if (installPathFileTypeDict[key].find("executable") >= 0 or
                    installPathFileTypeDict[key].find("ELF") >= 0 or
                    installPathFileTypeDict[key].find("directory") >= 0):
                g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, key, True)
            else:
                g_file.changeMode(DefaultValue.KEY_FILE_MODE, key)

    def __changeEnv(self):
        """
        function: Change GAUSS_ENV
        input : NA
        output: NA
        """
        # modified user's environmental variable $GAUSS_ENV
        self.logger.log("Modifying user's environmental variable $GAUSS_ENV.")
        DefaultValue.updateUserEnvVariable(self.mpprcFile, "GAUSS_ENV", "2")
        DefaultValue.updateUserEnvVariable(self.mpprcFile, "GS_CLUSTER_NAME",
                                           g_opts.clusterName)
        self.logger.log("Successfully modified user's environmental"
                        " variable $GAUSS_ENV.")

    def __fixFilePermission(self):
        """
        function: modify permission for app path
        input: NA
        ouput: NA
        """
        self.logger.log("Fixing file permission.")
        binPath = "'%s'/bin" % self.installPath
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, binPath, True)
        libPath = "'%s'/lib" % self.installPath
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, libPath, True)
        sharePath = "'%s'/share" % self.installPath
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, sharePath, True)
        etcPath = "'%s'/etc" % self.installPath
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, etcPath, True)
        includePath = "'%s'/include" % self.installPath
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, includePath, True)

        tarFile = "'%s'/bin/'%s'" % (self.installPath,
                                     DefaultValue.get_package_back_name())
        if (os.path.isfile(tarFile)):
            g_file.changeMode(DefaultValue.KEY_FILE_MODE, tarFile)

        # ./script/util/*.conf *.service
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/script/gspylib/etc/conf/check_list.conf"
                          % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/script/gspylib/etc/conf/"
                          "check_list_dws.conf" % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/script/gspylib/etc/conf/gs-OS-set.service"
                          % self.installPath)
        # bin config file
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/alarmItem.conf" % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/cluster_guc.conf" % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/upgrade_version" % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/retry_errcodes.conf" % self.installPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/bin/cluster_static_config" % self.installPath)

        # ./script/local/*.sql
        cmd = "find '%s'/bin/script -type f -name \"*.sql\" -exec" \
              " chmod 600 {} \\;" % self.installPath
        # ./lib files
        cmd += " && find '%s'/lib/ -type f -exec chmod 600 {} \\;" \
               % self.installPath
        # ./share files
        cmd += " && find '%s'/share/ -type f -exec chmod 600 {} \\;" \
               % self.installPath
        self.logger.debug("Command: %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            self.logger.log(output)
            self.logger.logExit(ErrorCode.GAUSS_501["GAUSS_50107"] % "app.")

    def installCluster(self):
        """
        function: install application
        input : NA
        output: NA
        """
        self.__decompressBinPackage()
        self.__saveUpgradeVerionInfo()
        self.__modifyAlarmItemConfFile()
        self.__createStaticConfig()
        if not self.dws_mode:
            self.__bakInstallPackage()
        self.__fixInstallPathPermission()
        self.__changeEnv()
        self.__fixFilePermission()

    def startCluster(self):
        """
        function: start cluster
        input: NA
        output: NA
        """
        for dn in self.dnCons:
            dn.start(self.time_out)

    def buildStandby(self):
        """
        function: build standby
        input: NA
        output: NA
        """
        for dn in self.dnCons:
            if dn.instInfo.instanceType == DefaultValue.STANDBY_INSTANCE:
                dn.build()

    def buildCascadeStandby(self):
        """
        function: build standby
        input: NA
        output: NA
        """
        for dn in self.dnCons:
            if dn.instInfo.instanceType == DefaultValue.CASCADE_STANDBY:
                dn.build_cascade()

    def cleanTempFile(self):
        """
        function: clean temp file
        input: NA
        output: NA
        """
        filename = "/tmp/temp.%s" % self.user
        try:
            if os.path.isfile(filename):
                g_file.removeFile(filename)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"]
                            % ("file [%s]" % filename))


if __name__ == '__main__':
    ##########################################################################
    # This is the main install flow.
    ##########################################################################
    """
    function: install the cluster
    input : NA
    output: NA
    """
    try:
        # Initialize self and Parse command line and save to global variable
        parseCommandLine()
        # check the parameters is not OK
        checkParameter()
        createLinkToApp()
        # Initialize globals parameters
        installer = Install(g_opts.logFile, g_opts.user, g_opts.clusterConfig,
                            g_opts.dws_mode, g_opts.mpprcFile,
                            g_opts.installPath, g_opts.alarmComponent,
                            g_opts.upgrade)
        installer.productVersion = g_opts.productVersion
        installer.time_out = g_opts.time_out
        try:
            functionDict = {ACTION_INSTALL_CLUSTER: installer.installCluster,
                            ACTION_START_CLUSTER: installer.startCluster,
                            ACTION_CLEAN_TEMP_FILE: installer.cleanTempFile,
                            ACTION_BUILD_STANDBY: installer.buildStandby,
                            ACTION_BUILD_CASCADESTANDBY:
                                installer.buildCascadeStandby}
            functionKeys = functionDict.keys()

            if g_opts.action in functionKeys:
                functionDict[g_opts.action]()
            else:
                g_opts.logger.logExit(ErrorCode.GAUSS_500["GAUSS_50004"] % 't'
                                      + " Value: %s." % g_opts.action)
        except Exception as e:
            g_opts.logger.log(traceback.format_exc())
            g_opts.logger.logExit(str(e))

        # close the log file
        g_opts.logger.closeLog()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
