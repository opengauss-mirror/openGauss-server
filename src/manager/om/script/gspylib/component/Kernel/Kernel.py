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
#############################################################################
import sys
import os
import subprocess
import grp
import pwd
import base64
import re
import time

sys.path.append(sys.path[0] + "/../../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.component.BaseComponent import BaseComponent
from gspylib.os.gsfile import g_file
from gspylib.common.Common import DefaultValue
from gspylib.threads.parallelTool import parallelTool, CommandThread
from gspylib.os.gsfile import g_file, g_Platform

RETRY_COUNT = 3
MAX_PARA_NUMBER = 1000


class Kernel(BaseComponent):
    '''
    The class is used to define base component.
    '''

    def __init__(self):
        """
        """
        super(Kernel, self).__init__()
        # init paramter schemaCoordinatorFile,
        # schemaJobFile and schemaDatanodeFile
        tmpDir = DefaultValue.getTmpDirFromEnv()
        self.schemaCoordinatorFile = "%s/%s" % (
            tmpDir, DefaultValue.SCHEMA_COORDINATOR)
        self.coordinatorJobDataFile = "%s/%s" % (
            tmpDir, DefaultValue.COORDINATOR_JOB_DATA)
        self.schemaDatanodeFile = "%s/%s" % (tmpDir,
                                             DefaultValue.SCHEMA_DATANODE)
        self.dumpTableFile = "%s/%s" % (tmpDir,
                                        DefaultValue.DUMP_TABLES_DATANODE)
        self.dumpOutputFile = "%s/%s" % (tmpDir,
                                         DefaultValue.DUMP_Output_DATANODE)
        self.coordinatorStatisticsDataFile = "%s/%s" % (
            tmpDir, DefaultValue.COORDINATOR_STAT_DATA)

    """
    Desc: 
        start/stop/query single instance 
    """

    def start(self, time_out=DefaultValue.TIMEOUT_CLUSTER_START,
              security_mode="off"):
        """
        """
        cmd = "%s/gs_ctl start -D %s " % (self.binPath, self.instInfo.datadir)
        if self.instInfo.instanceType == DefaultValue.MASTER_INSTANCE:
            if len(self.instInfo.peerInstanceInfos) > 0:
                cmd += "-M primary"
        elif self.instInfo.instanceType == DefaultValue.CASCADE_STANDBY:
            cmd += "-M cascade_standby"
        elif self.instInfo.instanceType == DefaultValue.STANDBY_INSTANCE:
            cmd += "-M standby"
        if time_out is not None:
            cmd += " -t %s" % time_out
        if security_mode == "on":
            cmd += " -o \'--securitymode\'"
        self.logger.debug("start cmd = %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0 or re.search("start failed", output):
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51607"] % "instance"
                            + " Error: Please check the gs_ctl log for "
                              "failure details." + "\n" + output)
        if re.search("another server might be running", output):
            self.logger.log(output)

    def stop(self, stopMode="", time_out=300):
        """
        """
        cmd = "%s/gs_ctl stop -D %s " % (
            self.binPath, self.instInfo.datadir)
        if not self.isPidFileExist():
            cmd += " -m immediate"
        else:
            # check stop mode
            if stopMode != "":
                cmd += " -m %s" % stopMode
        cmd += " -t %s" % time_out
        self.logger.debug("stop cmd = %s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51610"] %
                            "instance" + " Error: \n%s." % output)

    def isPidFileExist(self):
        pidFile = "%s/postmaster.pid" % self.instInfo.datadir
        return os.path.isfile(pidFile)

    def query(self):
        """
        """
        cmd = "%s/gs_ctl query -D %s" % (self.binPath, self.instInfo.datadir)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s " % output)
        return (status, output)

    def build(self, buidMode="full", standByBuildTimeout=300):
        """
        """
        cmd = "%s/gs_ctl build -D %s -M standby -b %s -r %d " % (
            self.binPath, self.instInfo.datadir, buidMode, standByBuildTimeout)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s " % output)
    def build_cascade(self, buidMode="full", standByBuildTimeout=300):
        """
        """
        cmd = "%s/gs_ctl build -D %s -M cascade_standby -b %s -r %d " % (
            self.binPath, self.instInfo.datadir, buidMode, standByBuildTimeout)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s " % output)

    def queryBuild(self):
        """
        """
        cmd = "%s/gs_ctl querybuild -D %s" % (self.binPath,
                                              self.instInfo.datadir)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s " % output)

    """
    Desc: 
        Under the AP branch, the installation package of each 
        component is not distinguished. 
        After checking, unzip the public installation package and 
        complete the installation. 
    """

    def install(self, nodeName="", dbInitParams=""):
        """
        """
        pass

    def getInstanceTblspcDirs(self, nodeName=""):
        """
        function: Get instance dirs 
        input : NA
        output: NA
        """
        tbsDirList = []

        if (not os.path.exists("%s/pg_tblspc" % self.instInfo.datadir)):
            self.logger.debug("%s/pg_tblspc does not exists." %
                              self.instInfo.datadir)
            return tbsDirList

        fileList = os.listdir("%s/pg_tblspc" % self.instInfo.datadir)
        if (len(fileList)):
            for filename in fileList:
                if (os.path.islink("%s/pg_tblspc/%s" % (self.instInfo.datadir,
                                                        filename))):
                    linkDir = os.readlink("%s/pg_tblspc/%s" % (
                        self.instInfo.datadir, filename))
                    if (os.path.isdir(linkDir)):
                        tblspcDir = "%s/%s_%s" % (
                            linkDir, DefaultValue.TABLESPACE_VERSION_DIRECTORY,
                            nodeName)
                        self.logger.debug("Table space directories is %s." %
                                          tblspcDir)
                        tbsDirList.append(tblspcDir)
                    else:
                        self.logger.debug(
                            "%s is not link directory." % linkDir)
                else:
                    self.logger.debug("%s is not a link file." % filename)
        else:
            self.logger.debug("%s/pg_tblspc is empty." % self.instInfo.datadir)

        return tbsDirList

    def getLockFiles(self):
        """
        function: Get lock files 
        input : NA
        output: NA
        """
        fileList = []
        # the static file must be exists
        tmpDir = os.path.realpath(DefaultValue.getTmpDirFromEnv())

        pgsql = ".s.PGSQL.%d" % self.instInfo.port
        pgsqlLock = ".s.PGSQL.%d.lock" % self.instInfo.port
        fileList.append(os.path.join(tmpDir, pgsql))
        fileList.append(os.path.join(tmpDir, pgsqlLock))
        return fileList

    def removeSocketFile(self, fileName):
        """
        """
        g_file.removeFile(fileName, "shell")

    def removeTbsDir(self, tbsDir):
        """
        """
        g_file.removeDirectory(tbsDir)

    def cleanDir(self, instDir):
        """
        function: Clean the dirs
        input : instDir
        output: NA
        """
        if (not os.path.exists(instDir)):
            return

        dataDir = []
        dataDir = os.listdir(instDir)
        if (os.getuid() == 0):
            pglDir = '%s/pg_location' % instDir
            isPglDirEmpty = False
            if (os.path.exists(pglDir) and len(os.listdir(pglDir)) == 0):
                isPglDirEmpty = True
            if (len(dataDir) == 0 or isPglDirEmpty):
                g_file.cleanDirectoryContent(instDir)
        else:
            for info in dataDir:
                if (str(info) == "pg_location"):
                    resultMount = []
                    resultFile = []
                    resultDir = []
                    pglDir = '%s/pg_location' % instDir

                    # delete all files in the mount point
                    cmd = "%s | %s '%s' | %s '{printf $3}'" % \
                          (g_Platform.getMountCmd(), g_Platform.getGrepCmd(),
                           pglDir, g_Platform.getAwkCmd())
                    (status, outputMount) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] %
                                        instDir + " Error:\n%s." %
                                        str(outputMount) +
                                        "The cmd is %s" % cmd)
                    else:
                        if (len(outputMount) > 0):
                            resultMount = str(outputMount).split()
                            for infoMount in resultMount:
                                g_file.cleanDirectoryContent(infoMount)
                        else:
                            g_file.cleanDirectoryContent(instDir)
                            continue

                    # delete file in the pg_location directory
                    if (not os.path.exists(pglDir)):
                        continue
                    cmd = "cd '%s'" % pglDir
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                        cmd + " Error: \n%s " % output)

                    outputFile = g_file.findFile(".", "f", "type")
                    if (len(outputFile) > 0):
                        for infoFile in outputFile:
                            tmpinfoFile = pglDir + infoFile[1:]
                            for infoMount in resultMount:
                                if (tmpinfoFile.find(infoMount) < 0 and
                                        infoMount.find(tmpinfoFile) < 0):
                                    realFile = "'%s/%s'" % (pglDir, infoFile)
                                    g_file.removeFile(realFile, "shell")

                    # delete directory in the pg_location directory
                    cmd = "if [ -d '%s' ]; then cd '%s' && find -type d; fi" \
                          % \
                          (pglDir, pglDir)
                    (status, outputDir) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] %
                                        instDir + " Error:\n%s." %
                                        str(outputDir) + "The cmd is %s" % cmd)
                    else:
                        resultDir = g_file.findFile(".", "d", "type")
                        resultDir.remove(".")
                        if (len(resultDir) > 0):
                            for infoDir in resultDir:
                                tmpinfoDir = pglDir + infoDir[1:]
                                for infoMount in resultMount:
                                    if (tmpinfoDir.find(infoMount) < 0 and
                                            infoMount.find(tmpinfoDir) < 0):
                                        realPath = "'%s/%s'" % (
                                        pglDir, infoDir)
                                        g_file.removeDirectory(realPath)
            cmd = "if [ -d '%s' ];then cd '%s' && find . ! -name " \
                  "'pg_location' " \
                  "! -name '..' ! -name '.' -print0 |xargs -r -0 -n100 rm " \
                  "-rf; " \
                  "fi" % (instDir, instDir)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] %
                                instDir + " Error:\n%s." % str(output) +
                                "The cmd is %s" % cmd)

    def uninstall(self, instNodeName):
        """
        function: Clean node instances.
                  1.get the data dirs, tablespaces, soketfiles
                  2.use theard delete the dirs or files
        input : instNodeName
        output: NA
        """
        self.logger.log("Cleaning instance.")

        # tablespace data directory
        tbsDirList = self.getInstanceTblspcDirs(instNodeName)

        # sockete file
        socketFiles = self.getLockFiles()

        # clean tablespace dir
        if (len(tbsDirList) != 0):
            try:
                self.logger.debug("Deleting instances tablespace directories.")
                for tbsDir in tbsDirList:
                    self.removeTbsDir(tbsDir)
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully cleaned instance tablespace.")

        if (len(self.instInfo.datadir) != 0):
            try:
                self.logger.debug("Deleting instances directories.")
                self.cleanDir(self.instInfo.datadir)
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully cleaned instances.")

        if (len(self.instInfo.xlogdir) != 0):
            try:
                self.logger.debug("Deleting instances xlog directories.")
                self.cleanDir(self.instInfo.xlogdir)
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully cleaned instances.")

        if (len(socketFiles) != 0):
            try:
                self.logger.debug("Deleting socket files.")
                for socketFile in socketFiles:
                    self.removeSocketFile(socketFile)
            except Exception as e:
                raise Exception(str(e))
            self.logger.log("Successfully cleaned socket files.")

    def setCommonItems(self):
        """
        function: set common items
        input : tmpDir
        output: tempCommonDict
        """
        tempCommonDict = {}
        tmpDir = DefaultValue.getTmpDirFromEnv()
        tempCommonDict["unix_socket_directory"] = "'%s'" % tmpDir
        tempCommonDict["unix_socket_permissions"] = "0700"
        tempCommonDict["log_file_mode"] = "0600"
        tempCommonDict["enable_nestloop"] = "off"
        tempCommonDict["enable_mergejoin"] = "off"
        tempCommonDict["explain_perf_mode"] = "pretty"
        tempCommonDict["log_line_prefix"] = "'%m %c %d %p %a %x %n %e '"
        tempCommonDict["modify_initial_password"] = "true"

        return tempCommonDict

    def doGUCConfig(self, action, GUCParasStr, isHab=False):
        """
        """
        # check instance data directory
        if (self.instInfo.datadir == "" or not os.path.exists(
                self.instInfo.datadir)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            ("data directory of the instance[%s]" %
                             str(self.instInfo)))

        if (GUCParasStr == ""):
            return

        # check conf file
        if (isHab == True):
            configFile = "%s/pg_hba.conf" % self.instInfo.datadir
        else:
            configFile = "%s/postgresql.conf" % self.instInfo.datadir
        if (not os.path.exists(configFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % configFile)

        cmd = "%s/gs_guc %s -D %s %s " % (self.binPath, action,
                                          self.instInfo.datadir, GUCParasStr)
        (status, output) = DefaultValue.retryGetstatusoutput(cmd, 3, 3)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50007"] % "GUC" +
                            " Command: %s. Error:\n%s" % (cmd, output))

    def setGucConfig(self, paraDict=None, setMode='set'):
        """
        """
        i = 0
        GUCParasStr = ""
        GUCParasStrList = []
        if paraDict is None:
            paraDict = {}
        for paras in paraDict:
            i += 1
            GUCParasStr += " -c \"%s=%s\" " % (paras, paraDict[paras])
            if (i % MAX_PARA_NUMBER == 0):
                GUCParasStrList.append(GUCParasStr)
                i = 0
                GUCParasStr = ""
        if (GUCParasStr != ""):
            GUCParasStrList.append(GUCParasStr)

        for parasStr in GUCParasStrList:
            self.doGUCConfig(setMode, parasStr, False)

    def removeIpInfoOnPghbaConfig(self, ipAddressList):
        """
        """
        i = 0
        GUCParasStr = ""
        GUCParasStrList = []
        for ipAddress in ipAddressList:
            i += 1
            GUCParasStr += " -h \"host    all    all    %s/32\"" % (ipAddress)
            if (i % MAX_PARA_NUMBER == 0):
                GUCParasStrList.append(GUCParasStr)
                i = 0
                GUCParasStr = ""
        if (GUCParasStr != ""):
            GUCParasStrList.append(GUCParasStr)

        for parasStr in GUCParasStrList:
            self.doGUCConfig("set", parasStr, True)
