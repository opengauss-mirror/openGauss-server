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


class CheckDiskFormat(BaseItem):
    def __init__(self):
        super(CheckDiskFormat, self).__init__(self.__class__.__name__)

    def doCheck(self):
        self.result.val = ""
        self.result.raw = ""
        xfs_mounts = list()
        expectedOption = "inode64"

        allDiskList = g_disk.getMountInfo()
        for disk in allDiskList:
            if (disk.fstype == 'xfs'):
                xfs_mounts.append(disk)
        informationlist = []
        if xfs_mounts == []:
            self.result.rst = ResultStatus.OK
            self.result.val = \
                "There is no XFS-formatted disk on the current node."
            return
        for disk in xfs_mounts:
            if disk.fstype != "xfs":
                informationlist.append(
                    "The device '%s' is not XFS filesystem "
                    "and is expected to be so." % disk.device)
                continue
            is_find = "failed"
            self.result.raw += "[%s]: type='%s' opts='%s'" % (
                disk.device, disk.fstype, disk.opts)
            for opt in disk.opts.split(','):
                if (opt == expectedOption):
                    is_find = "success"
                    break
                else:
                    continue
            if (is_find == "failed"):
                informationlist.append(
                    "XFS filesystem on device %s "
                    "is missing the recommended mount option '%s'." % (
                        disk.device, expectedOption))
                self.result.rst = ResultStatus.WARNING
        if (len(informationlist) != 0):
            for info in informationlist:
                self.result.val = self.result.val + '%s' % info
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "All XFS-formatted disk " \
                              "is normal on the current node."
