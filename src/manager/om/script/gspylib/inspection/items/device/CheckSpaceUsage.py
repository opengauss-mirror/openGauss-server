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
import psutil
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gsdisk import g_disk


class CheckSpaceUsage(BaseItem):
    def __init__(self):
        super(CheckSpaceUsage, self).__init__(self.__class__.__name__)
        self.diskVailPGHOST = None
        self.diskVailGPHOME = None
        self.diskVailGAUSSHOME = None
        self.diskVailGAUSSLOG = None
        self.diskVailOS_TMP = None
        self.diskVailDATA = None
        self.Threshold_NG = None
        self.Threshold_Warning = None

    def preCheck(self):
        # check current node contains cn instances if not raise  exception
        super(CheckSpaceUsage, self).preCheck()
        # check the threshold was set correctly
        if (not self.threshold.__contains__('DiskVailPGHOST')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold  DiskVailPGHOST")
        if (not self.threshold.__contains__('DiskVailGPHOME')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold DiskVailGPHOME")
        if (not self.threshold.__contains__('DiskVailGAUSSHOME')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold DiskVailGAUSSHOME")
        if (not self.threshold.__contains__('DiskVailGAUSSLOG')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold DiskVailGAUSSLOG")
        if (not self.threshold.__contains__('DiskVailOS_TMP')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold DiskVailOS_TMP")
        if (not self.threshold.__contains__('DiskVailDATA')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold DiskVailDATA")

        if (not (self.threshold.__contains__('Threshold_NG') and
                 self.threshold.__contains__('Threshold_Warning'))):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] %
                            "The threshold Threshold_NG and Threshold_Warning")
        if (not self.threshold['Threshold_NG'].isdigit() or
                not self.threshold['Threshold_Warning'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] %
                            "The threshold Threshold_NG and Threshold_Warning")
        self.Threshold_NG = int(self.threshold['Threshold_NG'])
        self.Threshold_Warning = int(self.threshold['Threshold_Warning'])
        if (self.Threshold_NG < self.Threshold_Warning):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53015"]
                            % (self.Threshold_NG, self.Threshold_Warning))
        if (self.Threshold_NG > 99 or self.Threshold_Warning < 1):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53016"]
                            % (self.Threshold_NG, self.Threshold_Warning))

        self.diskVailPGHOST = self.threshold['DiskVailPGHOST']
        self.diskVailGPHOME = self.threshold['DiskVailGPHOME']
        self.diskVailGAUSSHOME = self.threshold['DiskVailGAUSSHOME']
        self.diskVailGAUSSLOG = self.threshold['DiskVailGAUSSLOG']
        self.diskVailOS_TMP = self.threshold['DiskVailOS_TMP']
        self.diskVailDATA = self.threshold['DiskVailDATA']

    def obtainDataDir(self, nodeInfo):
        dataDirList = {}
        dataDirList[DefaultValue.getEnv("PGHOST")] = ["PGHOST",
                                                      self.diskVailPGHOST]
        dataDirList[DefaultValue.getEnv("GPHOME")] = ["GPHOME",
                                                      self.diskVailGPHOME]
        dataDirList[DefaultValue.getEnv("GAUSSHOME")] = \
            ["GAUSSHOME", self.diskVailGAUSSHOME]
        dataDirList[DefaultValue.getEnv("GAUSSLOG")] = ["GAUSSLOG",
                                                        self.diskVailGAUSSLOG]
        dataDirList["/tmp"] = ["OS_TMP", self.diskVailOS_TMP]
        for inst in nodeInfo.datanodes:
            dataDirList[inst.datadir] = ["DN", self.diskVailDATA]

        return dataDirList

    def obtainDiskDir(self):
        cmd = "df -h -P | awk '{print $NF}'"
        output = SharedFuncs.runShellCmd(cmd)
        allDiskPath = output.split('\n')[1:]
        return allDiskPath

    def doCheck(self):
        flag = "Normal"
        resultStr = ""
        DiskList = []
        DiskInfoDict = {}
        pathDisk = {}
        if (self.cluster):
            pathDisk = self.obtainDataDir(
                self.cluster.getDbNodeByName(self.host))
            pathList = pathDisk.keys()
        else:
            pathList = self.obtainDiskDir()
        for path in pathList:
            diskName = g_disk.getMountPathByDataDir(path)
            usageInfo = g_disk.getDiskSpaceUsage(path)
            diskInfo = "%s %s%%" % (diskName, usageInfo)
            if (diskName in DiskList):
                continue
            else:
                DiskList.append(diskName)
            DiskInfoDict[usageInfo] = diskInfo
            rateNum = usageInfo
            if (rateNum > self.Threshold_NG):
                resultStr += \
                    "The usage of the device disk space[%s:%d%%] " \
                    "cannot be greater than %d%%.\n" % (
                        diskName, rateNum, self.Threshold_NG)
                flag = "Error"
            elif (rateNum > self.Threshold_Warning):
                resultStr += \
                    "The usage of the device disk space[%s:%d%%] " \
                    "cannot be greater than %d%%.\n" % (
                        diskName, rateNum, self.Threshold_Warning)
                if (flag == "Normal"):
                    flag = "Warning"

            if (pathDisk):
                if (diskInfo):
                    AvailableSpace = psutil.disk_usage(
                        path).free // 1024 // 1024 // 1024
                    minSpace_KB = float(pathDisk[path][1])
                    if (AvailableSpace < minSpace_KB):
                        resultStr += \
                            "The %s path [%s] where" \
                            " the disk available space[%.1fGB] is less than" \
                            " %.1fGB.\n" % (
                                pathDisk[path][0], path, AvailableSpace,
                                minSpace_KB)
                        flag = "Error"
                else:
                    raise Exception(ErrorCode.GAUSS_504["GAUSS_50413"]
                                    + "Error:\n%s" % diskInfo)
        self.result.val = resultStr
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk space are sufficient.\n"
        elif (flag == "Warning"):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.NG

        keys = DiskInfoDict.keys()
        sorted(keys)
        MaxDisk = list(map(DiskInfoDict.get, keys))[-1]
        MinDisk = list(map(DiskInfoDict.get, keys))[0]
        self.result.val += "\nDisk     Filesystem spaceUsage\nMax " \
                           "free %s\nMin free %s" % (MaxDisk, MinDisk)
        for diskInfo in list(map(DiskInfoDict.get, keys)):
            self.result.raw += "\n%s" % diskInfo
