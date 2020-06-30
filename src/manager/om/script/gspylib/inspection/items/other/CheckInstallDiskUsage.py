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

from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gsdisk import g_disk


class CheckInstallDiskUsage(BaseItem):
    def __init__(self):
        super(CheckInstallDiskUsage, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = "Normal"
        path = "/boot"
        # Check space usage
        rateNum = g_disk.getDiskSpaceUsage(path)
        self.result.raw += "[%s] space usage: %s%%\n" % (path, rateNum)
        if (rateNum > int(self.thresholdDn)):
            self.result.val += \
                "Path(%s) space usage(%d%%)     " \
                "Abnormal reason: The usage of the device disk space " \
                "cannot be greater than %s%%.\n" % (
                    path, rateNum, self.thresholdDn)
            flag = "Error"
        # Check inode usage
        diskName = g_disk.getMountPathByDataDir(path)
        diskType = g_disk.getDiskMountType(diskName)
        if (not diskType in ["xfs", "ext3", "ext4"]):
            self.result.val = \
                "Path(%s) inodes usage(%s)     Warning reason: " \
                "The file system type [%s] is unrecognized or not support. " \
                "Please check it.\n" % (
                    path, 0, diskType)
            self.result.raw = "[%s] disk type: %s\n" % (path, diskType)
            self.result.rst = ResultStatus.WARNING
            return
        rateNum = g_disk.getDiskInodeUsage(path)
        self.result.raw += "[%s] inode usage: %s%%\n" % (path, rateNum)
        if (rateNum > int(self.thresholdDn)):
            self.result.val += \
                "Path(%s) inode usage(%d%%)     Abnormal reason: " \
                "The usage of the device disk inode cannot be greater than" \
                " %s%%.\n" % (
                    path, rateNum, self.thresholdDn)
            flag = "Error"
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
            self.result.val = "Install disk space are sufficient.\n"
        else:
            self.result.rst = ResultStatus.NG
