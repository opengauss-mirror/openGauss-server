# coding: UTF-8
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
import csv
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode


class CheckSlowDisk(BaseItem):
    def __init__(self):
        super(CheckSlowDisk, self).__init__(self.__class__.__name__)
        self.max = None
        self.high = None

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__('max')
                or not self.threshold.__contains__('high')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "threshold")
        if (not self.threshold['max'].isdigit() or
                not self.threshold['high'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The threshold max and high")
        self.max = float(self.threshold['max'])
        self.high = float(self.threshold['high'])

    def doCheck(self):
        # Perform 60-pass disk data collection
        dic = {}
        slowDiskList = []
        cmd = "for varible1 in {1..30}; do iostat -d -x -k 1 1 " \
              "| grep -E -v \"Linux|Device\"|awk 'NF'" \
              "|awk '{print $1,$(NF-1)}'; " \
              "sleep 1;done"
        output = SharedFuncs.runShellCmd(cmd)
        for line in output.splitlines():
            diskname = line.split()[0]
            svctmValue = line.split()[1]
            if (diskname in dic.keys()):
                diskList = dic[diskname]
                diskList.append(float(svctmValue))
                dic[diskname] = diskList
            else:
                dic[diskname] = [float(svctmValue)]
        for diskname, svctmValues in dic.items():
            diskList = sorted(svctmValues)
            if (diskList[-1] > self.max and diskList[-10] > self.high):
                slowDiskList.append(diskname)
        if (slowDiskList):
            self.result.rst = ResultStatus.NG
            self.result.val = "Slow Disk Found:\n%s" % (
                "\n".join(slowDiskList))
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "No Slow Disk Found"
