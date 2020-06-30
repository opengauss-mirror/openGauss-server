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
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gsdisk import g_disk

TOTAL_THRESHOLD_NG = 500000000


class CheckInodeUsage(BaseItem):
    def __init__(self):
        super(CheckInodeUsage, self).__init__(self.__class__.__name__)
        self.Threshold_NG = None
        self.Threshold_Warning = None

    def preCheck(self):
        # check current node contains cn instances if not raise  exception
        super(CheckInodeUsage, self).preCheck()
        # check the threshold was set correctly
        if (not (self.threshold.__contains__(
                'Threshold_NG') and self.threshold.__contains__(
            'Threshold_Warning'))):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The threshold Threshold_NG"
                              " and Threshold_Warning")
        if (not self.threshold['Threshold_NG'].isdigit() or not self.threshold[
            'Threshold_Warning'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53014"]
                            % ("The threshold Threshold_NG[%s]"
                               " and Threshold_Warning[%s]" %
                               (self.Threshold_NG, self.Threshold_Warning)))
        self.Threshold_NG = int(self.threshold['Threshold_NG'])
        self.Threshold_Warning = int(self.threshold['Threshold_Warning'])
        if (self.Threshold_NG < self.Threshold_Warning):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53015"]
                            % (self.Threshold_NG, self.Threshold_Warning))
        if (self.Threshold_NG > 99 or self.Threshold_Warning < 1):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53016"]
                            % (self.Threshold_NG, self.Threshold_Warning))

    def obtainDataDir(self, nodeInfo):
        dataDirList = []
        for inst in nodeInfo.datanodes:
            dataDirList.append(inst.datadir)
        dataDirList.append(DefaultValue.getEnv("PGHOST"))
        dataDirList.append(DefaultValue.getEnv("GPHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSLOG"))
        dataDirList.append("/tmp")
        return dataDirList

    def obtainDiskDir(self):
        cmd = "df -h -P | awk '{print $6}'"
        output = SharedFuncs.runShellCmd(cmd)
        allDiskPath = output.split('\n')[1:]
        return allDiskPath

    def doCheck(self):
        flag = "Normal"
        resultStr = ""
        top = ""
        DiskList = []
        DiskInfoDict = {}
        if (self.cluster):
            pathList = self.obtainDataDir(
                self.cluster.getDbNodeByName(self.host))
        else:
            pathList = self.obtainDiskDir()

        for path in pathList:
            diskName = g_disk.getMountPathByDataDir(path)
            diskType = g_disk.getDiskMountType(diskName)
            if (not diskType in ["xfs", "ext3", "ext4"]):
                resultStr += \
                    "Path(%s) inodes usage(%s)     Warning reason: " \
                    "The file system type [%s] is unrecognized " \
                    "or not support. Please check it.\n" % (
                        path, 0, diskType)
                if (flag == "Normal"):
                    flag = "Warning"
                continue
            usageInfo = g_disk.getDiskInodeUsage(path)
            if (diskName in DiskList):
                continue
            else:
                DiskList.append(diskName)
            DiskInfoDict[usageInfo] = "%s %s%%" % (diskName, usageInfo)
            if (usageInfo > self.Threshold_NG):
                resultStr += "The usage of the device " \
                             "disk inodes[%s:%d%%] cannot be greater than" \
                             " %d%%.\n" % (
                                 diskName, usageInfo, self.Threshold_NG)
                flag = "Error"
            elif (usageInfo > self.Threshold_Warning):
                resultStr += \
                    "The usage of the device disk inodes[%s:%d%%] " \
                    "cannot be greater than %d%%.\n" % (
                        diskName, usageInfo, self.Threshold_Warning)
                if (flag == "Normal"):
                    flag = "Warning"
        self.result.val = resultStr
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk inodes are sufficient."
        elif (flag == "Warning"):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.NG

        keys = DiskInfoDict.keys()
        sorted(keys)
        self.result.raw = "diskname inodeUsage"
        for diskInfo in map(DiskInfoDict.get, keys):
            self.result.raw += "\n%s" % diskInfo
