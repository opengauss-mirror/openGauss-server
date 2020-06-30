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
# Description  : Backup.py is a local utility to backup binary file
# and parameter file
#############################################################################
import getopt
import os
import sys

sys.path.append(sys.path[0] + "/../")
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsfile import g_file

#######################################################################
# GLOBAL VARIABLES
#######################################################################
GTM_CONF = "gtm.conf"

POSTGRESQL_CONF = "postgresql.conf"
POSTGRESQL_HBA_CONF = "pg_hba.conf"
CM_SERVER_CONF = "cm_server.conf"
CM_AGENT_CONF = "cm_agent.conf"
HOSTNAME = DefaultValue.GetHostIpOrName()

g_clusterUser = ""
g_ignoreMiss = False


class OldVersionModules():
    def __init__(self):
        """
        function: constructor
        """
        self.oldDbClusterInfoModule = None
        self.oldDbClusterStatusModule = None


class LocalBackup(LocalBaseOM):
    """
    function: classdocs
    input : NA
    output: NA
    """

    def __init__(self, logFile="", user="", tmpBackupDir="", backupDir="", \
                 backupPara=False, backupBin=False, nodeName=""):
        """
        function: initialize variable
        input : user, tmpBackupDir, backupDir, backupPara,
        backupBin, logFile, nodeName
        output: parameter
        """
        LocalBaseOM.__init__(self, logFile, user)
        self.tmpBackupDir = tmpBackupDir
        self.backupDir = backupDir
        self.backupPara = backupPara
        self.backupBin = backupBin
        self.nodeName = nodeName
        self.installPath = ""
        self.__hostnameFile = None
        self.dbNodeInfo = None
        self.clusterInfo = None

        ##static parameter
        self.binTarName = "binary_%s.tar" % HOSTNAME
        self.paraTarName = "parameter_%s.tar" % HOSTNAME
        self.hostnameFileName = "HOSTNAME"

    ########################################################################
    # This is the main install flow.
    ########################################################################

    def run(self):
        """
        function: 1.parse config file
                  2.check the backup directory
                  3.do the backup
                  4.close log file
        input : NA
        output: NA
        """
        try:
            # parse config file
            self.parseConfigFile()
            # Checking backup directory
            self.checkBackupDir()
            # back up binary files and parameter file
            self.doBackup()
        except Exception as e:
            self.logger.logExit(str(e))
        # close log file
        self.logger.closeLog()

    def parseClusterInfoFromStaticFile(self):
        """
        function: 1.init the clusterInfo
                  2.get clusterInfo from static config file
        input : NA
        output: NA
        """
        try:
            self.readConfigInfo()
        except Exception as e:
            self.logger.debug(str(e))
            gaussHome = DefaultValue.getInstallDir(self.user)
            try:
                g_oldVersionModules = OldVersionModules()
                if (os.path.exists(
                        "%s/bin/script/util/DbClusterInfo.py" % gaussHome)):
                    sys.path.append(
                        os.path.dirname("%s/bin/script/util/" % gaussHome))
                else:
                    sys.path.append(os.path.dirname(
                        "%s/bin/script/gspylib/common/" % gaussHome))
                g_oldVersionModules.oldDbClusterInfoModule = __import__(
                    'DbClusterInfo')
                self.clusterInfo = \
                    g_oldVersionModules.oldDbClusterInfoModule.dbClusterInfo()
                self.clusterInfo.initFromStaticConfig(self.user)
            except Exception as e:
                self.logger.debug(str(e))
                try:
                    self.clusterInfo = dbClusterInfo()
                    self.clusterInfo.initFromStaticConfig(self.user)
                except Exception as e:
                    self.logger.logExit(str(e))

    def parseConfigFile(self):
        """
        function: 1.init the clusterInfo
                  2.get clusterInfo from static config file
                  3.obtain local installation path for backup
                  4.obtain user and group for backup
                  5.obtain local node information for backup
        input : NA
        output: NA
        """
        self.logger.log("Parsing the configuration file.")
        self.parseClusterInfoFromStaticFile()
        try:
            self.logger.log("Obtaining local installation path for backup.")
            self.installPath = os.path.realpath(self.clusterInfo.appPath)
            if (not os.path.exists(self.installPath)):
                raise Exception(
                    ErrorCode.GAUSS_502["GAUSS_50201"] % self.installPath)

            self.logger.debug(
                "Local installation path: %s." % self.installPath)
            if (self.dbNodeInfo is None):
                self.logger.log("Obtaining local node information for backup.")
                self.dbNodeInfo = self.clusterInfo.getDbNodeByName(HOSTNAME)
                self.logger.debug(
                    "Local node information: \n%s." % str(self.dbNodeInfo))
        except Exception as e:
            raise Exception(str(e))

        self.logger.log("Successfully parsed the configuration file.")

    def checkBackupDir(self):
        """
        function: 1.mkdir the tmp backup directory
                  2.check the tmp backup directory size
                  3.mkdir the backup directory
        input : NA
        output: NA
        """
        self.logger.log("Checking backup directory.")

        try:
            if (not os.path.exists(self.tmpBackupDir)):
                os.makedirs(self.tmpBackupDir,
                            DefaultValue.KEY_DIRECTORY_PERMISSION)
            needSize = DefaultValue.APP_DISK_SIZE
            vfs = os.statvfs(self.tmpBackupDir)
            availableSize = vfs.f_bavail * vfs.f_bsize // (1024 * 1024)
            # 100M for binary files and parameter files
            if (availableSize < needSize):
                raise Exception(ErrorCode.GAUSS_504["GAUSS_50400"] % (
                    self.tmpBackupDir, str(needSize)))
        except Exception as e:

            raise Exception(str(e))

        if (self.backupDir != ""):
            try:
                if (not os.path.exists(self.backupDir)):
                    os.makedirs(self.backupDir,
                                DefaultValue.KEY_DIRECTORY_PERMISSION)
            except Exception as e:
                raise Exception(
                    ErrorCode.GAUSS_502["GAUSS_50208"] % self.backupDir
                    + " Error:\n%s" % e)

        self.logger.log("Successfully checked backup directory.")

    def doBackup(self):
        """
        function: 1.back up binary files
                  2.back up parameter files
        input : NA
        output: NA
        """
        self.logger.log("Backing up files.")

        if self.backupBin:
            self.logger.log("Backing up binary files.")

            try:
                self.logger.debug(
                    "Installation path is %s." % self.installPath)
                if (len(os.listdir(self.installPath)) == 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50203"] % (
                            "installation path [%s]" % self.installPath))
                self.__tarDir(self.installPath, self.binTarName)
            except Exception as e:
                raise Exception(str(e))

            self.logger.log("Successfully backed up binary files.")

        if self.backupPara:
            self.logger.log("Backing up parameter files.")

            try:
                self.logger.debug(
                    "Creating temporary directory for all parameter files.")
                temp_dir = os.path.join(self.tmpBackupDir,
                                        "parameter_%s" % HOSTNAME)
                self.logger.debug("Temporary directory path: %s." % temp_dir)
                if (os.path.exists(temp_dir)):
                    file_list = os.listdir(temp_dir)
                    if (len(file_list) != 0):
                        self.logger.debug(
                            "The temporary directory "
                            "is not empty.\n%s\nRemove all files silently."
                            % file_list)
                        g_file.cleanDirectoryContent(temp_dir)
                else:
                    os.makedirs(temp_dir,
                                DefaultValue.KEY_DIRECTORY_PERMISSION)

                self.logger.debug("Creating hostname file.")
                hostnameFile = os.path.join(temp_dir, self.hostnameFileName)
                self.logger.debug(
                    "Register hostname file path: %s." % hostnameFile)
                g_file.createFileInSafeMode(hostnameFile)
                with open(hostnameFile, "w") as self.__hostnameFile:
                    hostName = DefaultValue.GetHostIpOrName()
                    self.__hostnameFile.write("%s" % hostName)
                    self.logger.debug("Flush hostname file.")
                    self.__hostnameFile.flush()
                self.__hostnameFile = None

                os.chmod(hostnameFile, DefaultValue.KEY_FILE_PERMISSION)

                self.logger.debug("Collecting parameter files.")
                for inst in self.dbNodeInfo.datanodes:
                    self.__collectParaFilesToTempDir(inst, temp_dir)

                self.logger.debug(
                    "Generating parameter files to be compressed.")
                self.__tarDir(temp_dir, self.paraTarName, True)

                self.logger.debug("Removing temporary directory.")
                g_file.removeDirectory(temp_dir)
            except Exception as e:
                g_file.removeDirectory(temp_dir)
                raise Exception(str(e))

            self.logger.log("Successfully backed up parameter files.")

        self.logger.log("Successfully backed up files.")

    def __collectParaFilesToTempDir(self, inst, temp_dir):
        """
        function: 1.check the instance directory
                  2.get the parameter file of instance
                  3.copy the parameter file to backup directory
        input : inst, temp_dir
        output: NA
        """
        if (not os.path.exists(inst.datadir) or len(
                os.listdir(inst.datadir)) == 0):
            if (g_ignoreMiss):
                self.logger.log(
                    "Data directory (%s) of instance (%s) "
                    "does not exist or is empty." % \
                    (inst.datadir, str(inst)))
                return
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % \
                                (("data directory [%s] of instance [%s]")
                                 % (inst.datadir, str(inst))))

        paraFileList = {}
        if (inst.instanceRole == DefaultValue.INSTANCE_ROLE_CMSERVER):
            paraFileList[CM_SERVER_CONF] = os.path.join(inst.datadir,
                                                        CM_SERVER_CONF)
        elif (inst.instanceRole == DefaultValue.INSTANCE_ROLE_CMAGENT):
            paraFileList[CM_AGENT_CONF] = os.path.join(inst.datadir,
                                                       CM_AGENT_CONF)
        elif (inst.instanceRole == DefaultValue.INSTANCE_ROLE_GTM):
            paraFileList[GTM_CONF] = os.path.join(inst.datadir, GTM_CONF)
        elif (inst.instanceRole == DefaultValue.INSTANCE_ROLE_COODINATOR):
            paraFileList[POSTGRESQL_CONF] = os.path.join(inst.datadir,
                                                         POSTGRESQL_CONF)
            paraFileList[POSTGRESQL_HBA_CONF] = os.path.join(
                inst.datadir, POSTGRESQL_HBA_CONF)
        elif (inst.instanceRole == DefaultValue.INSTANCE_ROLE_DATANODE):
            paraFileList[POSTGRESQL_CONF] = os.path.join(
                inst.datadir, POSTGRESQL_CONF)
            paraFileList[POSTGRESQL_HBA_CONF] = os.path.join(
                inst.datadir, POSTGRESQL_HBA_CONF)
        else:
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51204"] % (
                "specified", inst.instanceRole))

        for key in paraFileList:
            if (not os.path.exists(paraFileList[key])):
                self.logger.debug(
                    "The parameter path is: %s." % paraFileList[key])
                if (g_ignoreMiss):
                    self.logger.log(
                        "Parameter file of instance [%s] is not existed." % (
                            str(inst)))
                    return
                else:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % (
                            'parameter file of instance [%s]' % (
                        str(inst))))

        for key in paraFileList:
            backupFileName = "%d_%s" % (inst.instanceId, key)
            g_file.cpFile(paraFileList[key],
                          os.path.join(temp_dir, backupFileName))

    def __tarDir(self, targetDir, tarFileName, backParameter=False):
        """
        function: 1.use tar commonds compress the backup file
                  2.copy the tar file to currently performing the backup
        input : targetDir, tarFileName, backParameter
        output: NA
        """
        tarName = os.path.join(self.tmpBackupDir, tarFileName)
        tarDir = targetDir.split("/")[-1]
        path = os.path.realpath(os.path.join(targetDir, ".."))
        cmd = g_file.SHELL_CMD_DICT["compressTarFile"] % (
            path, tarName, tarDir, DefaultValue.KEY_FILE_MODE, tarName)
        (status, output) = DefaultValue.retryGetstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50227"] % (
                    "directory [%s] to [%s]" % \
                    (targetDir, tarName)) + " Error: \n%s" % output)

        if self.nodeName != "":
            # Only parameter backup
            # send  backup file which is compressed  to the node
            # that is currently performing the backup
            if backParameter:
                g_OSlib.scpFile(self.nodeName, tarName, self.tmpBackupDir)


##############################################################################
# Help context. U:R:oC:v:
##############################################################################
def usage():
    """
    function: usage
    input  : NA
    output : NA
    """
    print(
        "Backup.py is a local utility to backup binary file "
        "and parameter file.")
    print(" ")
    print("Usage:")
    print("python3 Backup.py --help")
    print(" ")
    print("Common options:")
    print("  -U                              the user of cluster.")
    print("  -P, --position=TEMPBACKUPPATH   the temp backup directory.")
    print("  -B, --backupdir=BACKUPPATH      the backup directory.")
    print("  -p, --parameter                 backup parameter files.")
    print("  -b, --binary_file               backup binary files.")
    print("  -i, --ingore_miss               ignore Backup entity miss.")
    print(
        "      --nodeName=HOSTNAME         the node that is "
        "currently performing the backup.")
    print("  -l, --logpath=LOGPATH           the log directory.")
    print("  -h, --help                      show this help, then exit.")
    print(" ")


def checkUserParameter():
    """
    function: check user parameter
    input : NA
    output: NA
    """
    if (g_clusterUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U' + ".")


def checkLogFile(logFile):
    """
    function: check log file
    input : NA
    output: NA
    """
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                            g_clusterUser, "", "")
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50213"] % "log")


def checkBackupPara(backupPara, backupBin):
    """
    function: check -P and -b parameter
    input : NA
    output: NA
    """
    if not backupPara and not backupBin:
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50001"] % 'P or -b' + ".")


def checkTmpBackupDir(tmpBackupDir):
    """
    function: check tmp backup directory
    input : NA
    output: NA
    """
    if (tmpBackupDir == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'P' + ".")


def main():
    """
    function: main function
             1.parse command line
             2.check if user exist and is the right user
             3.check log file
             4.check backupPara and backupBin
             5.check tmpBackupDir
             6.do backup
    input : NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:B:l:pbhi",
                                   ["position=", "backupdir=", \
                                    "nodeName=", "parameter", "binary_file",
                                    "logpath=", "help", "ingore_miss"])
    except getopt.GetoptError as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))
    if (len(args) > 0):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    global g_clusterUser
    global g_ignoreMiss
    tmpBackupDir = ""
    backupDir = ""
    backupPara = False
    backupBin = False
    logFile = ""
    nodeName = ""
    for key, value in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value.strip()
        elif (key == "-P" or key == "--position"):
            tmpBackupDir = value.strip()
        elif (key == "-B" or key == "--backupdir"):
            backupDir = value.strip()
        elif (key == "-p" or key == "--parameter"):
            backupPara = True
        elif (key == "-b" or key == "--binary_file"):
            backupBin = True
        elif (key == "-i" or key == "--ingore_miss"):
            g_ignoreMiss = True
        elif (key == "-l" or key == "--logpath"):
            logFile = value.strip()
        elif (key == "--nodeName"):
            nodeName = value.strip()
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % value)

        Parameter.checkParaVaild(key, value)

    if (g_ignoreMiss):
        gaussHome = DefaultValue.getEnv("GAUSSHOME")
        if not gaussHome:
            return

    # check if user exist and is the right user
    checkUserParameter()
    DefaultValue.checkUser(g_clusterUser, False)
    # check log file
    checkLogFile(logFile)
    # check backupPara and backupBin
    checkBackupPara(backupPara, backupBin)
    # check tmpBackupDir
    checkTmpBackupDir(tmpBackupDir)
    try:
        LocalBackuper = LocalBackup(logFile, g_clusterUser, tmpBackupDir,
                                    backupDir, backupPara, backupBin, nodeName)
        LocalBackuper.run()
    except Exception as e:
        GaussLog.exitWithError(str(e))


if __name__ == '__main__':
    """
    function: main function
    input : NA
    output: NA
    """
    main()
    sys.exit(0)
