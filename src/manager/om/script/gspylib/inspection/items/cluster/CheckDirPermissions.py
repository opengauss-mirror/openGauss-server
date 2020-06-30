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
import pwd
import grp
import subprocess
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

DIRECTORY_MODE = 750
g_result = []
g_chList = []


class CheckDirPermissions(BaseItem):
    def __init__(self):
        super(CheckDirPermissions, self).__init__(self.__class__.__name__)

    def obtainDataDirLength(self, nodeInfo):
        """
        function: Obtain data dir length
        input: NA
        output: int, list
        """
        # Get the longest path
        DirLength = 0
        dataDirList = []
        # Get the DB instance and the longest DB path
        for inst in nodeInfo.datanodes:
            dataDirList.append(inst.datadir)
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)
        # Get the CMserver instance and longest path in the CMserver, DN
        for inst in nodeInfo.cmservers:
            dataDirList.append(inst.datadir)
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)
        # Get the CMagent instance and longest path in the CM, DN
        for inst in nodeInfo.cmagents:
            dataDirList.append(inst.datadir)
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)
        # Get the CN instance and longest path in the CM, DN, CN
        for inst in nodeInfo.coordinators:
            dataDirList.append(inst.datadir)
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)
        # Get the GTM instance and longest path in the CM, DN, CN, GTM
        for inst in nodeInfo.gtms:
            dataDirList.append(inst.datadir)
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)
        # Get the ETCD instance and longest path in the all instance
        if (hasattr(nodeInfo, 'etcds')):
            for inst in nodeInfo.etcds:
                dataDirList.append(inst.datadir)
                if (len(inst.datadir) > DirLength):
                    DirLength = len(inst.datadir)

        return (DirLength, dataDirList)

    def checkDirWriteable(self, dirPath, user, flag=""):
        """
        function : Check if target directory is writeable for user.
        input : String,String
        output : boolean
        """
        return os.access(dirPath, os.W_OK)

    def checkSingleDirectoryPermission(self, singledir, desc,
                                       INDENTATION_VALUE_INT):
        """
        function: Check Directory Permissions
        input: String, String, int
        output: int
        """
        # The directory must be a folder
        if (not os.path.isdir(singledir)):
            g_result.append(
                "%s: Abnormal reason: Directory does not exist." % (
                        "%s directory(%s)" % (desc, singledir)).ljust(
                    INDENTATION_VALUE_INT))
            return -1
        # Gets the folder permissions
        currentPremission = int(oct(os.stat(singledir).st_mode)[-3:])
        # Check the write access and compare the permission size
        if (self.checkDirWriteable(singledir, self.user)
                and currentPremission <= DIRECTORY_MODE):

            g_result.append(
                "%s: Normal" % ("%s directory(%s) permissions %s" % (
                    desc, singledir, str(currentPremission))).ljust(
                    INDENTATION_VALUE_INT))
            return 0
        elif (currentPremission > DIRECTORY_MODE):
            g_result.append(
                "%s: Abnormal reason: Directory permission"
                " can not exceed 750."
                % (("%s directory(%s) permissions %s"
                    % (desc, singledir,
                       str(currentPremission))).ljust(INDENTATION_VALUE_INT)))
            return -1
        else:
            g_result.append(
                "%s: Abnormal reason: Directory is not writable for users."
                % ("%s directory(%s) permissions %s"
                   % (desc, singledir,
                      str(currentPremission))).ljust(INDENTATION_VALUE_INT))
            return -1

    def doCheck(self):
        global g_chList
        global g_result
        resultList = []
        g_result = []
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        tmpDir = DefaultValue.getEnv("PGHOST")
        logDir = DefaultValue.getEnv("GAUSSLOG")
        toolDir = DefaultValue.getEnv("GPHOME")
        (intervalLen, instList) = self.obtainDataDirLength(nodeInfo)
        if intervalLen < len(self.cluster.appPath):
            intervalLen = len(self.cluster.appPath)
        if intervalLen < len(logDir):
            intervalLen = len(logDir)
        INDENTATION_VALUE_INT = intervalLen + 44
        # Check the permissions for appPath
        resultList.append(
            self.checkSingleDirectoryPermission(self.cluster.appPath,
                                                "AppPath",
                                                INDENTATION_VALUE_INT))
        g_chList.append(self.cluster.appPath)
        # Check the permissions for tmpPath
        resultList.append(self.checkSingleDirectoryPermission(
            tmpDir, "Tmp", INDENTATION_VALUE_INT))
        # Check the permissions for logPath
        g_chList.append(tmpDir)
        resultList.append(self.checkSingleDirectoryPermission(
            logDir, "Log", INDENTATION_VALUE_INT))
        # Check the permissions for logPath
        g_chList.append(logDir)
        resultList.append(
            self.checkSingleDirectoryPermission(toolDir, "ToolPath",
                                                INDENTATION_VALUE_INT))
        # Check the permissions for all CMserver
        g_chList.append(toolDir)
        # Check the permissions for all DB instance
        for inst in nodeInfo.datanodes:
            resultList.append(
                self.checkSingleDirectoryPermission(inst.datadir, "DN",
                                                    INDENTATION_VALUE_INT))
            # Check the xlog permissions for all DB instance
            xlogDir = "%s/pg_xlog" % inst.datadir
            resultList.append(
                self.checkSingleDirectoryPermission(xlogDir, "DN Xlog",
                                                    INDENTATION_VALUE_INT))
            g_chList.append(inst.datadir)
            g_chList.append(xlogDir)
        if (-1 in resultList):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = ""
        for detail in g_result:
            self.result.val = self.result.val + '%s\n' % detail

    def doSet(self):
        resultStr = ""
        for dirName in g_chList:
            g_file.changeOwner(self.user, dirName, True)
            g_file.changeMode(DIRECTORY_MODE, dirName)
        self.result.val = "Set DirPermissions completely."
