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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gsdisk import g_disk


class CheckDataDiskUsage(BaseItem):
    def __init__(self):
        super(CheckDataDiskUsage, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = "Normal"
        resultStr = ""
        pathList = []
        if (not self.cluster):
            self.result.rst = ResultStatus.NG
            self.result.val = "The datanode information is none."
            return

        for inst in self.cluster.getDbNodeByName(self.host).datanodes:
            pathList.append(inst.datadir)
        cnInstList = self.cluster.getDbNodeByName(self.host).coordinators
        if (len(cnInstList) > 0):
            tblspcDir = os.path.join(cnInstList[0].datadir, 'pg_tblspc')
            tblspcList = os.listdir(tblspcDir)
            if (tblspcList):
                for tblspc in tblspcList:
                    tblspcPath = os.path.join(tblspcDir, tblspc)
                    if (os.path.islink(tblspcPath)):
                        pathList.append(os.path.realpath(tblspcPath))

        for path in pathList:
            rateNum = g_disk.getDiskSpaceUsage(path)
            self.result.raw += "[%s] space usage: %s%%\n" % (path, rateNum)
            if (rateNum > int(self.thresholdDn)):
                resultStr += \
                    "Path(%s) space usage(%d%%)     Abnormal reason: " \
                    "The usage of the device disk space " \
                    "cannot be greater than %s%%.\n" % (
                        path, rateNum, self.thresholdDn)
                flag = "Error"
            # Check inode usage
            diskName = g_disk.getMountPathByDataDir(path)
            diskType = g_disk.getDiskMountType(diskName)
            if (not diskType in ["xfs", "ext3", "ext4", "overlay"]):
                resultStr += \
                    "Path(%s) inodes usage(%s)     Warning reason: " \
                    "The file system type [%s] is unrecognized " \
                    "or not support. Please check it.\n" % (
                        path, 0, diskType)
                if (flag == "Normal"):
                    flag = "Warning"
                self.result.raw += "[%s] disk type: %s\n" % (path, diskType)
                continue
            rateNum = g_disk.getDiskInodeUsage(path)
            self.result.raw += "[%s] inode usage: %s%%\n" % (path, rateNum)
            if (rateNum > int(self.thresholdDn)):
                resultStr += \
                    "Path(%s) inode usage(%d%%)     Abnormal reason: " \
                    "The usage of the device disk inode " \
                    "cannot be greater than %s%%.\n" % (
                        path, rateNum, self.thresholdDn)
                flag = "Error"
        self.result.val = resultStr
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk space are sufficient.\n"
        else:
            self.result.rst = ResultStatus.NG
