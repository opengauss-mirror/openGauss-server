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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode


class CheckLargeFile(BaseItem):
    def __init__(self):
        super(CheckLargeFile, self).__init__(self.__class__.__name__)
        self.Threshold_SIZE = None

    def preCheck(self):
        super(CheckLargeFile, self).preCheck()
        if (not (self.threshold.__contains__('size'))):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The threshold size")
        self.Threshold_SIZE = (self.threshold['size'])

    def obtainDataDir(self, nodeInfo):
        dataDirList = []
        for inst in nodeInfo.datanodes:
            dataDirList.append(inst.datadir)
        return dataDirList

    def checkLargeFile(self, path):
        fileList = []
        failList = []
        cmd = "find %s -type f -size +%s" % (path, self.Threshold_SIZE)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 and output.find("Permission denied") > 0):
            for fileName in output.splitlines():
                if (fileName.find("Permission denied") > 0):
                    failList.append(fileName)
        else:
            for fileName in output.splitlines():
                fileList.append(os.path.join(path, fileName))
        return fileList, failList

    def doCheck(self):
        outputList = []
        failList = []
        pathList = []
        if (self.cluster):
            paths = self.obtainDataDir(
                self.cluster.getDbNodeByName(self.host))
        else:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "cluster")
        for path in paths:
            if (path):
                pathList.append(path)
        pool = ThreadPool(DefaultValue.getCpuSet())
        results = pool.map(self.checkLargeFile, pathList)
        pool.close()
        pool.join()

        for outlist, flist in results:
            if (outlist):
                outputList.extend(outlist)
            if (flist):
                failList.extend(flist)

        if (len(outputList) == 0 and len(failList) == 0):
            self.result.rst = ResultStatus.OK
            self.result.val = "No file more than %s" % self.Threshold_SIZE
        else:
            if (len(outputList) > 0):
                self.result.val = "Files more than %s:\n%s" % (
                    self.Threshold_SIZE, "\n".join(outputList))
                if (len(failList) > 0):
                    self.result.val = "Files more than %s:\n%s\n%s" % (
                        self.Threshold_SIZE, "\n".join(outputList),
                        "\n".join(failList))
            else:
                self.result.val = "%s" % ("\n".join(failList))
            self.result.rst = ResultStatus.NG
