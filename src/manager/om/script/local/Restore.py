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
# Description : Restore.py is a local utility to
# restore binary file and parameter file.
#############################################################################
import subprocess
import getopt
import os
import sys

sys.path.append(sys.path[0] + "/../")
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.os.gsfile import g_file

# init config file parameter
POSTGRESQL_CONF = "postgresql.conf"
POSTGRESQL_HBA_CONF = "pg_hba.conf"
HOSTNAME = DefaultValue.GetHostIpOrName()
# init global paramter
g_clusterUser = ""
g_ignoreMiss = False
g_staticFile = ""


class LocalRestore(LocalBaseOM):
    '''
    classdocs
    '''

    def __init__(self, logFile="", user="", restoreDir="", restorePara=False,
                 restoreBin=False):
        """
        function: Constructor
        input : logFile, user, restoreDir, restorePara, restoreBin
        output: NA
        """
        LocalBaseOM.__init__(self, logFile, user)
        self.restoreDir = restoreDir
        self.restorePara = restorePara
        self.restoreBin = restoreBin

        self.installPath = ""
        self.binExtractName = ""
        self.group = ""
        self.dbNodeInfo = None
        self.clusterInfo = None
        self.__hostNameFile = None

        # #static parameter
        # Use binary_$hostname/parameter_$hostname to
        # confirm the backup asked that
        self.binTarName = "binary_%s.tar" % HOSTNAME
        self.paraTarName = "parameter_%s.tar" % HOSTNAME
        self.hostnameFileName = "HOSTNAME"

    ##########################################################################
    # This is the main restore flow.
    ##########################################################################

    def run(self):
        """
        function: 1.parse the configuration file
                  2.check restored directory
                  3.restore files
        input : NA
        output: NA
        """
        try:
            self.logger.log("Executing the local restoration")
            self.parseConfigFile()
            self.checkRestoreDir()
            self.doRestore()
            self.logger.log("Successfully execute the local restoration.")
            self.logger.closeLog()
            sys.exit(0)
        except Exception as e:
            raise Exception(str(e))

    def parseConfigFile(self):
        """
        function: parse the configuration file:
                  1.get local installation path for restoration
                  2.Obtain user and group for restoration
                  3.Obtain the local node information for restoration
        input : NA
        output: NA
        """
        self.logger.log("Parsing the configuration file.")

        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromStaticConfig(self.user, g_staticFile)
            hostName = DefaultValue.GetHostIpOrName()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit(
                    ErrorCode.GAUSS_516["GAUSS_51619"] % hostName)
            # Getting local installation path for restoration.
            self.logger.log("Getting local installation path for restoration.")
            self.installPath = os.path.realpath(self.clusterInfo.appPath)
            self.binExtractName = self.installPath.split("/")[-1]
            self.logger.debug(
                "Local installation path: %s." % self.installPath)
        except Exception as e:
            raise Exception(str(e))

        self.logger.log("Successfully parsed the configuration file.")

    def checkRestoreDir(self):
        """
        function: check restored directory
        input : NA
        output: NA
        """
        self.logger.log("Checking restored directory.")

        try:
            if (not os.path.exists(self.restoreDir) or len(
                    os.listdir(self.restoreDir)) == 0):
                if (g_ignoreMiss):
                    self.logger.log(
                        "Restored directory does not exist or is empty.")
                    sys.exit(0)
                else:
                    raise Exception(ErrorCode.GAUSS_502[
                                        "GAUSS_50228"] % "restored directory" +
                                    " Error: \n%s" % self.restoreDir)
        except Exception as e:
            raise Exception(str(e))

        self.logger.log("Successfully checked restored directory.")

    def doRestore(self):
        """
        function: restore files
                  Restoring binary files:
                  1.decompress tar file
                  2.Check binary files
                  3.Create installation path
                  4.Restore binary files to install path
                  Restoring parameter files:
                  1.decompress tar file
                  2.delete temporary directory
                  3.extract parameter files to the temporary directory
                  4.check hostname and parameter
                  5.Restore parameter files
                  6.Remove the temporary directory
        input : NA
        output: NA
        """
        self.logger.log("Restoring files.")

        if self.restoreBin:
            self.logger.log("Restoring binary files.")
            try:
                # decompress tar file
                self.decompressTarFile("binary")

                # Checking binary files 
                self.logger.debug("Checking if binary files exist.")
                tarName = os.path.join(self.restoreDir, self.binTarName)
                if (not os.path.exists(tarName)):
                    raise Exception(
                        ErrorCode.GAUSS_502["GAUSS_50201"] % "Binary files")

                # Creating installation path
                self.logger.debug(
                    "Creating installation path if did not exist.")
                if (not os.path.exists(self.installPath)):
                    os.makedirs(self.installPath,
                                DefaultValue.KEY_DIRECTORY_PERMISSION)

                # Restore binary files to install path.
                self.logger.debug("Restore binary files to install path.")
                g_file.cleanDirectoryContent(self.installPath)
                cmd = g_file.SHELL_CMD_DICT["decompressTarFile"] % (
                self.restoreDir, tarName)
                cmd += " && "
                cmd += g_file.SHELL_CMD_DICT["copyFile"] % (
                "'%s'/*" % self.binExtractName, self.installPath)
                self.logger.debug(
                    "Command for restoring binary files:%s." % cmd)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50220"] % (
                                "binary files to install path[%s]" % \
                                self.installPath) + " Error: \n%s" % output)
                g_file.removeDirectory(
                    os.path.join(self.restoreDir, self.binExtractName))
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully restored binary files.")

        if self.restorePara:
            self.logger.log("Restoring parameter files.")
            # Restoring parameter files.
            try:
                # decompress tar file
                self.decompressTarFile("parameter")
                # delete temporary directory
                self.logger.debug(
                    "Delete temporary directory if it has existed.")
                temp_dir = os.path.join(self.restoreDir,
                                        "parameter_%s" % HOSTNAME)
                if (os.path.exists(temp_dir)):
                    g_file.removeDirectory(temp_dir)

                # extract parameter files to the temporary directory
                self.logger.debug(
                    "Extract parameter files to the temporary directory.")
                tarName = os.path.join(self.restoreDir, self.paraTarName)
                if (not os.path.exists(tarName)):
                    if (g_ignoreMiss):
                        self.logger.error(ErrorCode.GAUSS_502[
                                              "GAUSS_50201"]
                                          % "parameter files")
                        sys.exit(0)
                    else:
                        raise Exception(ErrorCode.GAUSS_502[
                                            "GAUSS_50201"] % "parameter files")

                cmd = g_file.SHELL_CMD_DICT["decompressTarFile"] % (
                self.restoreDir, tarName)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514[
                                        "GAUSS_51400"] % cmd
                                    + " Error: \n%s" % output)

                # check hostname
                self.logger.debug("Checking hostname.")
                self.__checkHostName(
                    "%s/%s" % (temp_dir, self.hostnameFileName))
                # check parameter
                self.logger.debug("Checking parameter files.")
                paraFileList = []
                self.__checkParaFiles(temp_dir, paraFileList)

                self.logger.debug("Restoring parameter files.")
                paraFileNum = len(paraFileList)
                for i in range(paraFileNum):
                    tarFileName, paraFilePath = paraFileList[i].split('|')
                    g_file.cpFile(os.path.join(temp_dir, tarFileName),
                                  paraFilePath)

                self.logger.debug("Remove the temporary directory.")
                g_file.removeDirectory(temp_dir)
            except Exception as e:
                g_file.removeDirectory(temp_dir)
                raise Exception(str(e))
            self.logger.log("Successfully restored parameter files.")

        self.logger.log("Successfully restored files.")

    def decompressTarFile(self, flag):
        """
        function: Decompress package on restore node
        input : flag
        output: NA
        """
        tarFile = "%s/%s.tar" % (self.restoreDir, flag)
        if (not os.path.exists(tarFile)):
            return
        # Decompress package on restore node
        cmd = g_file.SHELL_CMD_DICT["decompressTarFile"] % (
        self.restoreDir, tarFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_502[
                                "GAUSS_50217"] % tarFile
                            + " Error: \n%s." % output
                            + "The cmd is %s " % cmd)

    def __checkHostName(self, hostnameFile):
        """
        function: make sure the hostname stored in tar files
        input : hostnameFile
        output: NA
        """
        # make sure the hostname stored in tar files
        localHostName = DefaultValue.GetHostIpOrName()
        with open(hostnameFile, 'r') as self.__hostNameFile:
            storedHostName = self.__hostNameFile.read()
        storedHostName.strip('\n')
        if (((localHostName > storedHostName) - (
                localHostName < storedHostName)) != 0):
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51637"] % \
                            (("Local hostname [%s]",
                              "the hostname [%s] stored in tar files") % (
                             localHostName, storedHostName)))

    def __checkParaFiles(self, temp_dir, paraFileList):
        """
        function: check parameter file
        input : temp_dir, paraFileList
        output: NA
        """
        storedParaFileNum = len(os.listdir(temp_dir)) - 1

        for inst in self.dbNodeInfo.datanodes:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)
        if ((storedParaFileNum > len(paraFileList)) -
            (storedParaFileNum < len(paraFileList))) != 0:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51637"] %
                            ("number of parameter files",
                             "the number of files requested"))

    def __checkSingleParaFile(self, inst, temp_dir, paraFileList):
        """
        function: check single parameter file
        input : inst, temp_dir, paraFileList
        output: NA
        """
        # makesure instance exist
        if (not os.path.exists(inst.datadir)):
            if (g_ignoreMiss):
                self.logger.log(
                    "Data directory [%s] of instance [%s]does not exist." % (
                    inst.datadir, str(inst)))
                return
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % \
                                ("Data directory [%s] of instance [%s]" % (
                                inst.datadir, str(inst))))
        # get all parameter file path into paraFileMap
        paraFileMap = {}
        if inst.instanceRole == DefaultValue.INSTANCE_ROLE_DATANODE:
            paraFileMap[POSTGRESQL_CONF] = \
                os.path.join(inst.datadir, POSTGRESQL_CONF)
            paraFileMap[POSTGRESQL_HBA_CONF] = \
                os.path.join(inst.datadir, POSTGRESQL_HBA_CONF)
        else:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51204"] % (
            "specified", inst.instanceRole))

        for key in paraFileMap:
            backupFileName = "%d_%s" % (inst.instanceId, key)
            if (not os.path.exists(os.path.join(temp_dir, backupFileName))):
                if (g_ignoreMiss):
                    self.logger.log(
                        "The file of %s does not exist." % backupFileName)
                    return
                else:
                    raise Exception(
                        ErrorCode.GAUSS_502["GAUSS_50201"] % backupFileName)
            newRecord = "%s|%s" % (backupFileName, paraFileMap[key])
            paraFileList.append(newRecord)


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
        "Restore.py is a local utility to restore binary file "
        "and parameter file.")
    print(" ")
    print("Usage:")
    print("python3 Restore.py --help")
    print(" ")
    print("Common options:")
    print("  -U                              the user of cluster.")
    print("  -P, --position=RESTOREPATH      the restore directory.")
    print("  -p, --parameter                 restore parameter files.")
    print("  -b, --binary_file               restore binary files.")
    print("  -i, --ingore_miss               ignore Backup entity miss.")
    print("  -s, --static_file               static configuration files.")
    print("  -l, --logpath=LOGPATH           the log directory.")
    print("  -h, --help                      show this help, then exit.")
    print(" ")


def checkUserExist():
    """
    function: check user exists
    input : NA
    output: NA
    """
    if (g_clusterUser == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % "U" + ".")
    DefaultValue.checkUser(g_clusterUser, False)


def checkLogFile(logFile):
    """
    function: check log file
    input : NA
    output: NA
    """
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                            g_clusterUser, "")
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50213"] % "log")


def checkRestorePara(restorePara, restoreBin):
    """
    function: check restore parameter
    input : NA
    output: NA
    """
    if not restorePara and not restoreBin:
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50001"] % "p or -b" + ".")


def checkRestoreDir(restoreDir):
    """
    function: check restore directory
    input : NA
    output: NA
    """
    if (restoreDir == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % "P" + ".")


def main():
    """
    function: main function
    input : NA
    output: NA
    """

    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:l:pbhis:", \
                                   ["position=", "parameter", "binary_file",
                                    "logpath=", "help", "ingore_miss",
                                    "static_file="])
    except getopt.GetoptError as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % e.msg)
    if (len(args) > 0):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    global g_clusterUser
    global g_ignoreMiss
    global g_staticFile
    restoreDir = ""
    restorePara = False
    restoreBin = False
    logFile = ""

    for key, value in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value.strip()
        elif (key == "-P" or key == "--position"):
            restoreDir = value.strip()
        elif (key == "-p" or key == "--parameter"):
            restorePara = True
        elif (key == "-b" or key == "--binary_file"):
            restoreBin = True
        elif (key == "-i" or key == "--ingore_miss"):
            g_ignoreMiss = True
        elif (key == "-s" or key == "--static_file"):
            g_staticFile = value.strip()
        elif (key == "-l" or key == "--logpath"):
            logFile = value
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % value)

        Parameter.checkParaVaild(key, value)

    if (g_ignoreMiss):
        gaussHome = DefaultValue.getEnv("GAUSSHOME")
        if not gaussHome:
            return

    # check if user exist and is the right user
    checkUserExist()
    # check log file
    checkLogFile(logFile)
    # check -p and -b
    checkRestorePara(restorePara, restoreBin)
    # check -P
    checkRestoreDir(restoreDir)

    try:
        LocalRestorer = LocalRestore(logFile, g_clusterUser, restoreDir,
                                     restorePara, restoreBin)
        LocalRestorer.run()
    except Exception as e:
        GaussLog.exitWithError(str(e))


if __name__ == '__main__':
    main()
