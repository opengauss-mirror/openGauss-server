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
# Description  : CheckUpgrade.py is a utility to check the env before upgrade.
#############################################################################
import getopt
import sys
import os
import subprocess
import pwd
import re
import traceback

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
import impl.upgrade.UpgradeConst as Const

INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2

#############################################################################
# Global variables
#############################################################################
g_logger = None
g_clusterInfo = None


class CmdOptions():
    """
    Class for defining some cmd options
    """

    def __init__(self):
        self.action = ""
        # the current old appPath
        self.appPath = ""
        self.user = ""
        self.logFile = ""
        self.xmlFile = ""
        self.upgrade_version = ""
        self.newAppPath = ""


class CheckUpgrade():
    """
    Class to Check application setting for upgrade
    """

    def __init__(self, appPath, action, newAppPath):
        '''
        Constructor
        '''
        self.appPath = appPath
        self.action = action
        self.newAppPath = newAppPath

    def run(self):
        """
        function: Check upgrade environment
        input: NA
        output: NA
        """
        self.__checkSHA256()
        self.__checkAppPath()
        self.__checkDataDir()
        if self.action == Const.ACTION_INPLACE_UPGRADE:
            self.__checkBackupDir()
            self.__checkAppVersion()
            self.__backupDbClusterInfo()

    def __checkAppPath(self):
        """
        function: check app path
        input: NA
        output: NA
        """
        if not os.path.isdir(self.appPath):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"]
                             % self.appPath)

        static_config = "%s/bin/cluster_static_config" % self.appPath
        if not os.path.exists(static_config):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"]
                             % static_config)
        if not os.path.isfile(static_config):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50210"]
                             % static_config)

        if not os.path.isdir(self.newAppPath):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"]
                             % self.newAppPath)
        if os.path.samefile(self.newAppPath, self.appPath):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50233"]
                             % ("install path", "$GAUSSHOME"))

        # check if the current app path is correct size,
        # there should be no personal data
        cmd = "du -hms %s | awk '{print $1}'" % os.path.realpath(self.appPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                             + " ERROR: %s" % str(output))
        appSize = output

        cmd = "du -hms %s/lib/postgresql/pg_plugin | awk '{print $1}'" \
              % os.path.realpath(self.appPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                             " ERROR: %s" % str(output))
        pluginSize = output
        if int(appSize) - int(pluginSize) > Const.MAX_APP_SIZE:
            g_logger.logExit(ErrorCode.GAUSS_504["GAUSS_50401"]
                             % (self.appPath, "%dM" % Const.MAX_APP_SIZE) +
                             "\nThere may be personal data in path %s,"
                             " please move your data to other directory"
                             % self.appPath)

    def __checkAppVersion(self):
        """
        function: Check version
        input: NA
        output: NA
        """
        # grey upgrade no need do this check
        curVer = DefaultValue.getAppVersion(self.appPath)
        if (curVer == ""):
            g_logger.logExit(ErrorCode.GAUSS_516["GAUSS_51623"])

        gaussHome = DefaultValue.getEnvironmentParameterValue("GAUSSHOME",
                                                              g_opts.user)
        if not gaussHome:
            g_logger.logExit(ErrorCode.GAUSS_518["GAUSS_51800"]
                             % "$GAUSSHOME")

        gaussdbFile = "%s/bin/gaussdb" % gaussHome
        cmd = "%s --version 2>/dev/null" % (gaussdbFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                             + "Error:\n %s" % output)

    def __checkSHA256(self):
        '''
        function: Check the sha256 of new version
        input: NA
        output: NA
        '''
        try:
            DefaultValue.checkPackageOS()
        except Exception as e:
            g_logger.logExit(str(e))

    def __getTmpDir(self):
        """
        """
        return DefaultValue.getTmpDirFromEnv()

    def __getBackupDir(self):
        """
        """
        return "%s/binary_upgrade" % DefaultValue.getTmpDirFromEnv()

    def __getGaussdbVersion(self, gaussdbFile):
        """
        """
        # backup gaussdb version
        # get old cluster version by gaussdb
        # the information of gaussdb like this:
        #    gaussdb Gauss200 V100R00XCXX build xxxx
        #    compiled at xxxx-xx-xx xx:xx:xx
        if (not os.path.isfile(gaussdbFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % gaussdbFile)

        oldClusterVersion = ""
        cmd = "%s --version" % (gaussdbFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0 and None is not re.compile(
                r'V[0-9]{3}R[0-9]{3}C[0-9]{2}').search(str(output))):
            oldClusterVersion = re.compile(
                r'V[0-9]{3}R[0-9]{3}C[0-9]{2}').search(str(output)).group()
        else:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "\nOutput:%s" % output)
        return oldClusterVersion

    def __backupDbClusterInfo(self):
        """
        function: backup DbClusterInfo.py and cluster_static_config to temp
                  path
        input: NA
        output: NA
        """
        commonStaticConfigFile = "%s/bin/cluster_static_config" \
                                 % g_opts.appPath
        commonUpgradeVersionFile = "%s/bin/upgrade_version" % g_opts.appPath
        commonDbClusterInfoModule = \
            "%s/bin/script/gspylib/common/DbClusterInfo.py" % g_opts.appPath

        bakPath = self.__getTmpDir()

        # backup DbClusterInfo.py
        oldDbClusterInfoModule = "%s/OldDbClusterInfo.py" % bakPath
        cmd = "cp -p '%s'  '%s'" % (commonDbClusterInfoModule,
                                    oldDbClusterInfoModule)
        cmd += " && cp -rp '%s/bin/script/' '%s'" % (g_opts.appPath, bakPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                             + "\nOutput:%s" % output)

        # backup cluster_static_config
        cmd = "cp -p '%s' '%s'/" % (commonStaticConfigFile, bakPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                             + "\nOutput:%s" % output)

        upgradeBakPath = self.__getBackupDir()
        try:
            # backup upgrade_version
            oldUpgradeVersionFile = "%s/old_upgrade_version" % upgradeBakPath
            cmd = "cp -p %s  %s" % (commonUpgradeVersionFile,
                                    oldUpgradeVersionFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "\nOutput:%s" % output)

        except Exception as e:
            g_logger.debug("Backup failed.ERROR:%s\nClean backup path."
                           % str(e))
            if (os.path.isdir(upgradeBakPath)):
                g_file.removeDirectory(upgradeBakPath)

    def __checkBackupDir(self):
        """
        for binary upgrade, Check if backup dir exists, it may be not empty,
         because in the second time, we may have
        file record app dir and record node app
        INPUT:NA
        OUTPUT:NA
        HIDEN:
        1.paths need to be baked
        2.100M
        PRECONDITION:
        POSTCONDITION:
        2.for binary upgrade, bak dir has been ready
        TEST:
        Pseudocode:
        """
        binaryBakDir = "%s/binary_upgrade" % DefaultValue.getTmpDirFromEnv()
        if not os.path.isdir(binaryBakDir):
            os.makedirs(binaryBakDir, DefaultValue.KEY_DIRECTORY_PERMISSION)

        vfs = os.statvfs(binaryBakDir)
        availableSize = vfs.f_bavail * vfs.f_bsize / (1024 * 1024)
        g_logger.debug("The available size of backup directory: %d M."
                       % availableSize)
        if(availableSize < Const.MAX_APP_SIZE):
            g_logger.logExit(ErrorCode.GAUSS_504["GAUSS_50400"]
                             % ("BakDir", "%dM" % Const.MAX_APP_SIZE))

    def __checkDataDir(self):
        """
        function: check data directory access rights.
        input: NA
        output:NA
        """
        g_logger.debug("Checking data directory access rights.")

        instDirs = self.getNodeDirs()
        for path in instDirs:
            if (not os.path.exists(path)):
                g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
            if (not g_file.checkDirWriteable(path)):
                g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50205"] % path)

        g_logger.debug("Successfully to check data directory access rights.")

    def getNodeDirs(self, pathType=""):
        """
        function: get the install path and data path of cluster
                  1. collect install path
                  2. collect data path
                  3. collect tablespc path
                  4. remove the same items
        input : pathType
        output : tempPaths
        """
        localHost = DefaultValue.GetHostIpOrName()
        dbNode = g_clusterInfo.getDbNodeByName(localHost)
        if not dbNode:
            g_logger.logExit(ErrorCode.GAUSS_512["GAUSS_51209"]
                             % ("NODE", localHost))
        newNodePaths = []
        DnInfos = []
        tablespacePaths = []

        # collect install path
        newNodePaths.append(g_clusterInfo.appPath)

        for instance in dbNode.datanodes:
            newNodePaths.append(instance.datadir)
            if (instance.instanceType != DUMMY_STANDBY_INSTANCE):
                DnInfos.append(instance.datadir)
                if ('ssdDir' in dir(instance)):
                    if (len(instance.ssdDir) != 0):
                        newNodePaths.append(instance.ssdDir)

        # collect tablespc path
        for instanceDir in DnInfos:
            if (not os.path.exists("%s/pg_tblspc" % instanceDir)):
                g_logger.debug("%s/pg_tblspc does not exist." % instanceDir)
                continue
            fileList = os.listdir("%s/pg_tblspc" % instanceDir)
            if (len(fileList)):
                for filename in fileList:
                    if (os.path.islink("%s/pg_tblspc/%s"
                                       % (instanceDir, filename))):
                        linkDir = os.readlink("%s/pg_tblspc/%s"
                                              % (instanceDir, filename))
                        if (os.path.isdir(linkDir)):
                            tablespacePaths.append(linkDir)
                        else:
                            g_logger.debug("%s is not a link directory."
                                           % filename)
                    else:
                        g_logger.debug("%s is not a link file." % filename)
            else:
                g_logger.debug("%s/pg_tblspc is empty." % instanceDir)

        if (pathType == "tablespace"):
            tempPaths = tablespacePaths
        else:
            tempPaths = newNodePaths + tablespacePaths

        # remove the same items
        tempPaths = list(set(tempPaths))

        return tempPaths


def usage():
    """
Usage:
  python3 CheckUpgrade.py -t action -R installpath -N newClusterInstallPath
   [-U user] [-l log]
Common options:
  -t                                the type of action
  -l                                the path of log file
  --help                            show this help, then exit
Options for big version upgrade check
  -U                                the user of old cluster
  -X                                path of the XML configuration file
Options for upgrade check
  -R                                the install path of old cluster
  -N                                the install path of new cluster
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: Parse command line and save to global variable
    input: NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:R:U:v:l:X:N:", ["help"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-R"):
            g_opts.appPath = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-l"):
            g_opts.logFile = os.path.realpath(value)
        elif (key == "-X"):
            g_opts.xmlFile = os.path.realpath(value)
        elif (key == "-v"):
            g_opts.upgrade_version = value
        elif (key == "-N"):
            g_opts.newAppPath = value
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % key)

        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: Parse command line and save to global variable
    input: NA
    output: NA
    """
    # only check need parameter, just ignore no need parameter
    if (g_opts.user == ""):
        g_opts.user = pwd.getpwuid(os.getuid()).pw_name

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, "", g_opts.appPath, "")

    if g_opts.action in [Const.ACTION_LARGE_UPGRADE,
                         Const.ACTION_SMALL_UPGRADE,
                         Const.ACTION_INPLACE_UPGRADE]:
        if (g_opts.appPath == ""):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                                   % "R" + ".")

    elif(g_opts.action == Const.ACTION_CHECK_VERSION):
        if(g_opts.upgrade_version == ""):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                                   % "v" + ".")
    else:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"] % 't'
                               + " Value: %s." % g_opts.action)


def checkVersion():
    """
    function: check version information
    input: NA
    output: NA
    """
    g_logger.debug("Checking version information.")
    gaussHome = DefaultValue.getInstallDir(g_opts.user)
    if gaussHome == "":
        raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$GAUSSHOME")
    localPostgresVersion = \
        DefaultValue.getAppBVersion(os.path.realpath(gaussHome))
    if (localPostgresVersion.find(g_opts.upgrade_version) > 0):
        g_logger.debug("Successfully checked version information.")
    else:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52935"])


class OldVersionModules():
    """
    Class for providing some functions to apply old version cluster
    """

    def __init__(self):
        '''
        Constructor
        '''
        self.oldDbClusterInfoModule = None
        self.oldDbClusterStatusModule = None


def initGlobalInfos():
    """
    function: init global infos
    input: NA
    output: NA
    """
    global g_logger
    global g_clusterInfo
    g_logger = GaussLog(g_opts.logFile, "CheckUpgrade")
    try:
        if g_opts.action in [Const.ACTION_CHECK_VERSION]:
            g_logger.log("No need to init cluster info under action %s"
                         % g_opts.action)
            return
        g_clusterInfo = dbClusterInfo()
        if g_opts.xmlFile == "" or not os.path.exists(g_opts.xmlFile):
            if g_opts.appPath == "" or not os.path.exists(g_opts.appPath):
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50001"] % "R")
            staticConfigFile = "%s/bin/cluster_static_config" % g_opts.appPath
            g_clusterInfo.initFromStaticConfig(g_opts.user, staticConfigFile)
        else:
            g_clusterInfo.initFromXml(g_opts.xmlFile)
    except Exception as e:
        g_logger.log(traceback.format_exc())
        g_logger.logExit(str(e))


if __name__ == '__main__':
    """
    main function
    1. parse command
    2. check other parameter
    3. init global infos
    4. check version information
    5. Check application setting for upgrade
    """
    try:
        g_opts = CmdOptions()
        # 1. parse command
        parseCommandLine()
        # 2. check other parameter
        checkParameter()
        # 3. init global infos
        initGlobalInfos()
    except Exception as e:
        GaussLog.exitWithError(str(e) + traceback.format_exc())

    try:
        # 4. check version information
        if(g_opts.action == Const.ACTION_CHECK_VERSION):
            checkVersion()
        else:
            # 5. Check application setting for upgrade
            g_logger.log("Checking upgraded environment.")
            checker = CheckUpgrade(g_opts.appPath, g_opts.action,
                                   g_opts.newAppPath)
            checker.run()
            g_logger.log("Successfully checked upgraded environment.")
        g_logger.closeLog()
    except Exception as e:
        g_logger.log(traceback.format_exc())
        g_logger.logExit(str(e))

    sys.exit(0)
