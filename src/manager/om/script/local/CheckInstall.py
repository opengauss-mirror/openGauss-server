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
# Description  : CheckInstall.py is a utility to install Gauss MPP Database.
#############################################################################
import getopt
import os
import sys
import platform
import math

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.VersionInfo import VersionInfo
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.os.gsfile import g_file
from gspylib.os.gsOSlib import g_OSlib
from gspylib.threads.parallelTool import parallelTool

OTHER_FLAG = "0"
PREINSTALL_FLAG = "1"
INSTALL_FLAG = "2"
g_clusterInfo = None
TIME_OUT = 2
RETRY_TIMES = 3

########################################################################
# Global variables define
########################################################################
g_opts = None


########################################################################
class CmdOptions():
    """
    Class: cmdOptions
    """

    def __init__(self):
        """
        function: Constructor
        input : NA
        output: NA
        """
        self.installPath = ""
        self.user = ""
        self.group = ""
        self.userProfile = ""
        self.mpprcFile = ""
        self.clusterConfig = ""
        self.logFile = ""
        self.userInfo = ""

        # DB config parameters
        self.confParameters = []
        self.platformString = platform.system()
        self.logger = None


#######################################################################
# Help context. U:R:oC:v:
########################################################################
def usage():
    """
python3 checkInstall.py is a utility to check Gauss MPP Database install
env.
Usage:
  python3 checkInstall.py --help
  python3 checkInstall.py -U user:group -R installpath [--replace]

Common options:
  -U        the database program and cluster owner
  -R        the database program path
  -C        configure the database configuration file, for more detail
  information see postgresql.conf
  --replace do check install for replace
  --help    show this help, then exit
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    parse command line
    input : NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:R:C:l:X:", ["help"])
    except getopt.GetoptError as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))
    if (len(args) > 0):
        usage()
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    parameter_map = {"-U": g_opts.userInfo, "-R": g_opts.installPath,
                     "-l": g_opts.logFile, "-X": g_opts.clusterConfig}
    parameter_keys = parameter_map.keys()
    for key, value in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key in parameter_keys):
            parameter_map[key] = value
        elif (key == "-C"):
            g_opts.confParameters.append(value)
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % value)
        Parameter.checkParaVaild(key, value)

    g_opts.userInfo = parameter_map["-U"]
    g_opts.installPath = parameter_map["-R"]
    if os.path.islink(g_opts.installPath) or not os.path.exists(
            g_opts.installPath):
        versionFile = VersionInfo.get_version_file()
        commitid = VersionInfo.get_version_info(versionFile)[2]
        g_opts.installPath = g_opts.installPath + "_" + commitid
    g_opts.logFile = parameter_map["-l"]
    g_opts.clusterConfig = parameter_map["-X"]


def checkParameter():
    """
    function: 1.check input parameters
              2.check user info
              3.check os user
              4.check log file info
              5.check mpprc file path
              6.check configFile
              7.check install path
    input : NA
    output: NA
    """
    # check user info
    checkUser(g_opts.userInfo)

    # check mpprc file path
    g_opts.mpprcFile = DefaultValue.getMpprcFile()
    checkOSUser()

    # check log file info
    checkLogFile(g_opts.logFile)

    # check configFile
    checkXMLFile()

    # check install path
    checkInstallPath()


def checkUser(userInfo):
    """
    function: check user
    input : userInfo
    output: NA
    """
    if (userInfo == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")

    strList = userInfo.split(":")
    if (len(strList) != 2):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50009"])
    g_opts.user = strList[0].strip()
    g_opts.group = strList[1].strip()
    if (g_opts.user == "" or g_opts.group == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"] % "U")


def checkOSUser():
    """
    function: 1.use linux commands 'id -gn' get the user's group
              2.check the user's group match with the input group
              3.get user's env file
    input : NA
    output: NA
    """
    try:
        group = g_OSlib.getGroupByUser(g_opts.user)
    except Exception as e:
        GaussLog.exitWithError(str(e))
    if (group != g_opts.group):
        GaussLog.exitWithError(ErrorCode.GAUSS_503["GAUSS_50305"])

    # get user env file
    g_opts.userProfile = g_opts.mpprcFile


def checkLogFile(logFile):
    """
    function: check log file
    input : logFile
    output: NA
    """
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                            g_opts.user, "", "")
    g_opts.logger = GaussLog(logFile, "CheckInstall")


def checkXMLFile():
    """
    function: check configuration file
             1.check -X parameter
             2.check configuration file exists
             3.check configuration file an absolute path
    input : NA
    output: NA
    """
    if (g_opts.clusterConfig != ""):
        if (not os.path.exists(g_opts.clusterConfig)):
            g_opts.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50201"] % g_opts.clusterConfig)
        if (not os.path.isabs(g_opts.clusterConfig)):
            g_opts.logger.logExit(
                ErrorCode.GAUSS_512["GAUSS_51206"] % g_opts.clusterConfig)


def checkPath(path_type_in):
    """
    function: Check the path:
              the path must be composed of letters, numbers,
              underscores, slashes, hyphen, and spaces
    input : path_type_in
    output: NA
    """
    pathLen = len(path_type_in)
    i = 0
    a_ascii = ord('a')
    z_ascii = ord('z')
    A_ascii = ord('A')
    Z_ascii = ord('Z')
    num0_ascii = ord('0')
    num9_ascii = ord('9')
    blank_ascii = ord(' ')
    sep1_ascii = ord('/')
    sep2_ascii = ord('_')
    sep3_ascii = ord('-')
    sep4_ascii = ord('.')
    for i in range(0, pathLen):
        char_check = ord(path_type_in[i])
        if (not (
                a_ascii <= char_check <= z_ascii
                or A_ascii <= char_check <= Z_ascii
                or num0_ascii <= char_check <= num9_ascii
                or char_check == blank_ascii
                or char_check == sep1_ascii
                or char_check == sep2_ascii
                or char_check == sep3_ascii
                or char_check == sep4_ascii)):
            return False
    return True


def checkInstallPath():
    """
    function: check installation path
    input : NA
    output: NA
    """
    if (g_opts.installPath == ""):
        g_opts.logger.logExit(ErrorCode.GAUSS_500["GAUSS_50001"] % 'R' + ".")
    g_opts.installPath = os.path.normpath(g_opts.installPath)
    g_opts.installPath = os.path.realpath(g_opts.installPath)
    if (not os.path.isdir(os.path.realpath(g_opts.installPath))):
        g_opts.logger.logExit(
            ErrorCode.GAUSS_502["GAUSS_50201"] % g_opts.installPath)
    if (not checkPath(g_opts.installPath)):
        g_opts.logger.logExit(ErrorCode.GAUSS_512["GAUSS_51235"] %
                              g_opts.installPath + " The path must be "
                                                   "composed of"
                                                   "letters, numbers,"
                                                   "underscores,"
                                                   "slashes, hyphen, "
                                                   "and spaces."
                              )

    g_opts.logger.debug(
        "Using installation program path: %s." % g_opts.installPath)
    g_opts.logger.debug("Using set configuration file parameters: %s." % str(
        g_opts.confParameters))


def checkOldInstallStatus():
    """
    function: Check old database install.
              If this user have old install, report error and exit.
    input : NA
    output: NA
    """
    g_opts.logger.log("Checking old installation.")
    # Check $GAUSS_ENV.
    try:
        gauss_ENV = DefaultValue.getEnvironmentParameterValue("GAUSS_ENV",
                                                              g_opts.user)
        if (str(gauss_ENV) == str(INSTALL_FLAG)):
            g_opts.logger.logExit(ErrorCode.GAUSS_518["GAUSS_51806"])
    except Exception as ex:
        g_opts.logger.logExit(str(ex))
    g_opts.logger.log("Successfully checked old installation.")


def checkSHA256():
    """
    function: Check the sha256 number for database install binary file.
    input : NA
    output: NA
    """
    g_opts.logger.log("Checking SHA256.")
    try:
        DefaultValue.checkPackageOS()
    except Exception as e:
        g_opts.logger.logExit(str(e))
    g_opts.logger.log("Successfully checked SHA256.")


def getFileInfo(fileName):
    """
    function:
    input : filename
    output: file context
    """
    res = g_file.readFile(fileName)
    if (len(res) != 1):
        raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] % fileName)
    return res[0].strip()


def checkOSKernel():
    """
    function: Check OS kernel parameters: share memory size and semaphore.(
    postgresql.conf/gtm.conf)
              1.check shared_buffers
              2.check sem
    input : NA
    output: NA
    """
    g_opts.logger.log("Checking kernel parameters.")
    # GB MB kB
    GB = 1 * 1024 * 1024 * 1024
    MB = 1 * 1024 * 1024
    kB = 1 * 1024
    shared_buffers = 1 * GB
    max_connections = 800

    for item in g_opts.confParameters:
        tmp = item.strip()
        listname = tmp.split("=")
        try:
            if (((listname[0].lower() > "shared_buffers") - (
                    listname[0].lower() < "shared_buffers")) == 0):
                if listname[1][0:-2].isdigit() and (
                        (listname[1][-2:] > "GB") - (
                        listname[1][-2:] < "GB")) == 0:
                    shared_buffers = int(listname[1][0:-2]) * GB
                if listname[1][0:-2].isdigit() and (
                        (listname[1][-2:] > "MB") - (
                        listname[1][-2:] < "MB")) == 0:
                    shared_buffers = int(listname[1][0:-2]) * MB
                if listname[1][0:-2].isdigit() and (
                        (listname[1][-2:] > "kB") - (
                        listname[1][-2:] < "kB")) == 0:
                    shared_buffers = int(listname[1][0:-2]) * kB
                if listname[1][0:-1].isdigit() and (
                        (listname[1][-2:] > "B") - (
                        listname[1][-2:] < "B")) == 0:
                    shared_buffers = int(listname[1][0:-1])
            if (((listname[0].lower() > "max_connections") - (
                    listname[0].lower() < "max_connections")) == 0):
                if listname[1].isdigit():
                    max_connections = int(listname[1])
        except ValueError as ex:
            g_opts.logger.logExit(ErrorCode.GAUSS_500[
                                      "GAUSS_50010"] % "kernel" +
                                  "Error:\n%s" % str(
                ex))

    # check shared_buffers
    if (shared_buffers < 128 * kB):
        g_opts.logger.logExit(
            ErrorCode.GAUSS_504["GAUSS_50400"] % ("Shared_buffers", "128KB"))

    try:
        shmaxFile = "/proc/sys/kernel/shmmax"
        shmallFile = "/proc/sys/kernel/shmall"
        shmmax = getFileInfo(shmaxFile)
        shmall = getFileInfo(shmallFile)
        PAGESIZE = g_OSlib.getSysConfiguration()
        if (shared_buffers > int(shmmax)):
            g_opts.logger.logExit(ErrorCode.GAUSS_505["GAUSS_50501"])
        if (shared_buffers > int(shmall) * int(PAGESIZE)):
            g_opts.logger.logExit(ErrorCode.GAUSS_504["GAUSS_50401"] % (
            "Shared_buffers", "shmall*PAGESIZE"))
    except ValueError as ex:
        g_opts.logger.logExit(str(ex))

    try:
        semFile = "/proc/sys/kernel/sem"
        semList = getFileInfo(semFile)
        paramList = semList.split("\t")
        if (int(paramList[0]) < 17):
            g_opts.logger.logExit(
                ErrorCode.GAUSS_524["GAUSS_52401"] % ("SEMMSL", "SEMMSL") +
                paramList[0] + ". Please check it.")
        if (int(paramList[3]) < math.ceil((max_connections + 150) // 16)):
            g_opts.logger.logExit(
                ErrorCode.GAUSS_524["GAUSS_52401"] % ("SEMMNI", "SEMMNI") +
                paramList[3] + ". Please check it.")
        if (int(paramList[1]) < math.ceil((max_connections + 150) // 16) * 17):
            g_opts.logger.logExit(
                ErrorCode.GAUSS_524["GAUSS_52401"] % ("SEMMNS", "SEMMNS") +
                paramList[1] + ". Please check it.")
    except ValueError as ex:
        g_opts.logger.logExit(str(ex))
    g_opts.logger.log("Successfully checked kernel parameters.")


def checkInstallDir():
    """
    function: Check database program file install directory size.
              The free space size should not smaller than 100M.
              1.check if install path exists
              2.check install path is empty or not
              3.check install path uasge
    input : NA
    output: NA
    """
    g_opts.logger.log("Checking directory.")

    # check if install path exists
    if (not os.path.exists(g_opts.installPath)):
        g_opts.logger.logExit(ErrorCode.GAUSS_502[
                                  "GAUSS_50201"] % g_opts.installPath +
                              "\nPlease create it first.")

    # check install path is empty or not.
    installFileList = os.listdir(g_opts.installPath)
    for oneFile in installFileList:
        if (oneFile == "full_upgrade_bak" and os.path.isdir(
                "%s/%s" % (g_opts.installPath, oneFile))):
            continue
        elif (oneFile == "lib" and os.path.isdir(
                "%s/%s" % (g_opts.installPath, oneFile))):
            libList = os.listdir("%s/%s" % (g_opts.installPath, oneFile))
            if (len(libList) == 1 and libList[
                0] == "libsimsearch" and os.path.isdir(
                    "%s/%s/%s" % (g_opts.installPath, oneFile, libList[0]))):
                continue
            else:
                g_opts.logger.logExit(
                    ErrorCode.GAUSS_502["GAUSS_50202"] % g_opts.installPath)
        elif (oneFile == "bin" and os.path.isdir(
                "%s/%s" % (g_opts.installPath, oneFile))):
            binFieList = os.listdir("%s/%s" % (g_opts.installPath, oneFile))
            for binFie in binFieList:
                if (binFie.find("cluster_static_config") < 0):
                    g_opts.logger.logExit(ErrorCode.GAUSS_502[
                                              "GAUSS_50202"] %
                                          g_opts.installPath)
        elif (oneFile == "secbox" and os.path.isdir(
                "%s/%s" % (g_opts.installPath, oneFile))):
            continue
        else:
            g_opts.logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50202"] % g_opts.installPath)

    # check install path uasge
    vfs = os.statvfs(g_opts.installPath)
    availableSize = vfs.f_bavail * vfs.f_bsize // (1024 * 1024)

    g_opts.logger.log(
        "Database program installation path available size %sM." % str(
            availableSize))
    if (availableSize < 100):
        g_opts.logger.logExit(
            ErrorCode.GAUSS_504["GAUSS_50400"] % (g_opts.installPath, "100M"))

    g_opts.logger.log("Successfully checked directory.")


class CheckInstall(LocalBaseOM):
    """
    Class: check install
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
        else:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromXml(self.clusterConfig)
            hostName = DefaultValue.GetHostIpOrName()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit(
                    ErrorCode.GAUSS_516["GAUSS_51619"] % hostName)
        # get user info
        self.getUserInfo()
        if (user != "" and self.user != user.strip()):
            self.logger.debug("User parameter : %s." % user)
            self.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50315"] % (
            self.user, self.clusterInfo.appPath))
        # init every component
        self.initComponent()

    def checkPortAndIp(self):
        """
        function: Check instance port and IP
        input : NA
        output: NA
        """
        self.logger.log("Checking instance port and IP.")
        components = self.etcdCons + self.cmCons + self.gtmCons \
                     + self.cnCons + self.dnCons
        try:
            # config instance in paralle
            parallelTool.parallelExecute(self.checkIpAndPort, components)
        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.log("Successfully checked instance port and IP.")

    def checkIpAndPort(self, component):
        """
        function: Check instance port and IP for per component
        input : component
        output: NA
        """
        component.perCheck()

    def checkPreEnv(self):
        """
        function: Check if LD path and path in preinstall had been changed.
        input : NA
        output: NA
        """
        g_opts.logger.log("Checking preinstall enviroment value.")
        # Check $GAUSS_ENV.
        try:
            # get mpp file by env parameter MPPDB_ENV_SEPARATE_PATH
            mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
            if (mpprcFile != "" and mpprcFile is not None):
                userProfile = mpprcFile
                if (not os.path.isabs(userProfile)):
                    raise Exception(
                        ErrorCode.GAUSS_512["GAUSS_51206"] % userProfile)
                if (not os.path.exists(userProfile)):
                    raise Exception(
                        ErrorCode.GAUSS_502["GAUSS_50201"] % userProfile)
            else:
                userProfile = "/etc/profile"
            reEnvList = g_file.readFile(userProfile)
            checkList = [
                "export PATH=$GPHOME/script/gspylib/pssh/bin:$GPHOME/script"
                ":$PATH",
                "export LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH"]
            for check in checkList:
                if (check not in reEnvList and (
                        check + '\n') not in reEnvList):
                    self.logger.logExit(
                        ErrorCode.GAUSS_518["GAUSS_51802"] % check)
        except Exception as e:
            g_opts.logger.logExit(str(e))
        g_opts.logger.log("Successfully checked preinstall enviroment value.")


if __name__ == '__main__':
    ###################################################################
    # check install
    ###################################################################
    """
    function:   Check all kinds of environment. It includes:
                1. Input parameters.
                2. OS version .
                3. If it has a old install.
                4. OS kernel parameters.
                5. Install directory size and stauts.
                6. Security.
                7. Binary file integrity verify.
    input : NA
    output: NA
    """
    try:
        # parse and check input parameters
        parseCommandLine()
        checkParameter()
        # Check whether the old database installed
        checkOldInstallStatus()
        # Check the sha256 number for database install binary file
        checkSHA256()

        if (g_opts.platformString == "Linux"):
            # Check OS kernel parameters: share memory size
            # and semaphore.(postgresql.conf/gtm.conf)
            checkOSKernel()
            # Check database program file install directory size
            checkInstallDir()

        if (g_opts.clusterConfig != ""):
            # Check instance port and IP
            checker = CheckInstall(g_opts.logFile, g_opts.user,
                                   g_opts.clusterConfig)
            checker.checkPortAndIp()
            checker.checkPreEnv()

        g_opts.logger.closeLog()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
