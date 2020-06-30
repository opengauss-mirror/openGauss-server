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
import subprocess
from multiprocessing.dummy import Pool as ThreadPool
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file


class CheckSpecialFile(BaseItem):
    def __init__(self):
        super(CheckSpecialFile, self).__init__(self.__class__.__name__)

    def getDiskPath(self):
        nodeDirs = []
        # get PGHOST Dir
        tmpDir = DefaultValue.getEnv("PGHOST")
        nodeDirs.append(tmpDir)

        # get gphome dir
        gphome_path = DefaultValue.getEnv("GPHOME")
        nodeDirs.append(gphome_path)

        # get log dir
        log_path = DefaultValue.getEnv("GAUSSLOG")
        nodeDirs.append(log_path)

        # get gausshome dir
        gausshome_path = DefaultValue.getEnv("GAUSSHOME")
        nodeDirs.append(os.path.realpath(gausshome_path))

        hostName = DefaultValue.GetHostIpOrName()
        dbNode = self.cluster.getDbNodeByName(hostName)
        # including dn
        for dbInst in dbNode.datanodes:
            nodeDirs.append(dbInst.datadir)

        return nodeDirs

    def checkPathVaild(self, envValue):
        """
        function: check path vaild
        input : envValue
        output: NA
        """
        if (envValue.strip() == ""):
            return 0
        # check path vaild
        for rac in DefaultValue.PATH_CHECK_LIST:
            flag = envValue.find(rac)
            if flag >= 0:
                return 1
        return 0

    def ignorePath(self, path):
        # Part of the root path and file permissions need to be ignored
        ignorePathList = []
        toolPath = DefaultValue.getEnv("GPHOME")
        sudoPath = os.path.join(toolPath, "sudo")
        inspectionPath = os.path.join(toolPath, "script/inspection")
        ignorePathList.append("%s/script/gs_preinstall" % toolPath)
        ignorePathList.append("%s/script/gs_postuninstall" % toolPath)
        ignorePathList.append("%s/script/gs_checkos" % toolPath)

        scriptPath = os.path.join(toolPath, "script")
        scriptDirList = scriptPath.split('/')
        inspectionDirList = inspectionPath.split('/')
        # ignore own special files
        if (path in ignorePathList or os.path.dirname(path) == sudoPath):
            return True
        else:
            (filename, suffix) = os.path.splitext(path)
            pathDirList = path.split('/')
            # ignore .pyc file in GPHOME/script
            if (path.find(scriptPath) == 0 and pathDirList[:len(
                    scriptDirList)] == scriptDirList and suffix == ".pyc"):
                return True
            # ignore GPHOME/script/inspection dir
            elif (path.find(inspectionPath) == 0 and pathDirList[:len(
                    inspectionDirList)] == inspectionDirList):
                return True
            else:
                return False

    def checkSpecialChar(self):
        outputList = []
        failList = []
        pathList = []
        paths = self.getDiskPath()
        for path in paths:
            if (not path or not os.path.isdir(path)):
                continue
            else:
                pathList.append(path)
        pool = ThreadPool(DefaultValue.getCpuSet())
        results = pool.map(self.checkSingleSpecialChar, pathList)
        pool.close()
        pool.join()
        for outlist, flist in results:
            if (outlist):
                outputList.extend(outlist)
            if (flist):
                failList.extend(flist)
        if (len(outputList) > 0):
            outputList = DefaultValue.Deduplication(outputList)
        if (failList):
            failList = DefaultValue.Deduplication(failList)
        return outputList, failList

    def checkSingleSpecialChar(self, path):
        # Check a single path
        outputList = []
        failList = []
        cmd = "find '%s' -name '*'" % path
        (status, output) = subprocess.getstatusoutput(cmd)
        FileList = output.split('\n')
        while '' in FileList:
            FileList.remove('')
        if (status != 0 and output.find("Permission denied") > 0):
            for realPath in FileList:
                if (realPath.find("Permission denied") > 0):
                    failList.append(realPath)
                elif (self.checkPathVaild(realPath) != 0):
                    outputList.append(realPath)
        else:
            for realPath in FileList:
                if (self.checkPathVaild(realPath) != 0):
                    outputList.append(realPath)
        return outputList, failList

    #########################################################
    # get the files which under the all useful directory and
    # its owner is not current execute use
    #########################################################
    def checkErrorOwner(self, ownername):
        outputList = []
        failList = []
        path = ""
        for path in self.getDiskPath():
            if (not path or not os.path.isdir(path)):
                continue
            cmd = "find '%s' -iname '*' ! -user %s -print" % (path, ownername)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0 and output != ""):
                pathList = output.split("\n")
                for path in pathList:
                    if (self.ignorePath(path)):
                        continue
                    outputList.append(path)
            elif (output.find("Permission denied") > 0):
                pathList = output.split("\n")
                for path in pathList:
                    if (path.find("Permission denied") > 0):
                        failList.append(path)
                        continue
                    if (self.ignorePath(path)):
                        continue
                    outputList.append(path)
        if (len(outputList) > 0):
            outputList = DefaultValue.Deduplication(outputList)
        return outputList, failList

    def doCheck(self):
        parRes = ""
        flag = 0
        output = ""
        outputList, failList = self.checkSpecialChar()
        for output in outputList:
            if (output != ""):
                flag = 1
                parRes += "\nSpecial characters file: \"%s\"" % output

        outputList, errorList = self.checkErrorOwner(self.user)
        for output in outputList:
            if (output != ""):
                flag = 1
                parRes += "\nFile owner should be %s." \
                          " Incorrect owner file: \"%s\"" \
                          % (self.user, output)
        failList.extend(errorList)
        if (failList):
            flag = 1
            failList = DefaultValue.Deduplication(failList)
            parRes += "\n%s" % ("\n".join(failList))
        if (flag == 1):
            self.result.rst = ResultStatus.NG
            self.result.val = parRes
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "All files are normal."
