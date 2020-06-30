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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode


class CheckCPU(BaseItem):
    def __init__(self):
        super(CheckCPU, self).__init__(self.__class__.__name__)
        self.idle = None
        self.wio = None
        self.standard = None

    def preCheck(self):
        # check the threshold was set correctly
        if (not "StandardCPUIdle" in self.threshold.keys()
                or not "StandardWIO" in self.threshold.keys()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "threshold")
        if (not self.threshold['StandardCPUIdle'].isdigit() or not
        self.threshold['StandardWIO'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53014"]
                            % "The threshold StandardCPUIdle and StandardWIO")
        self.idle = self.threshold['StandardCPUIdle']
        self.wio = self.threshold['StandardWIO']

        # format the standard by threshold
        self.standard = self.standard.decode('utf-8').format(idle=self.idle,
                                                             iowait=self.wio)

    def doCheck(self):
        cmd = "sar 1 5 2>&1"
        output = SharedFuncs.runShellCmd(cmd)
        self.result.raw = output
        # check the result with threshold
        d = next(n.split() for n in output.splitlines() if "Average" in n)
        iowait = d[-3]
        idle = d[-1]
        rst = ResultStatus.OK
        vals = []
        if (float(iowait) > float(self.wio)):
            rst = ResultStatus.NG
            vals.append(
                "The %s actual value %s %% is greater than "
                "expected value %s %%" % (
                    "IOWait", iowait, self.wio))
        if (float(idle) < float(self.idle)):
            rst = ResultStatus.NG
            vals.append(
                "The %s actual value %s %% is less than "
                "expected value %s %%" % (
                    "Idle", idle, self.idle))
        self.result.rst = rst
        if (vals):
            self.result.val = "\n".join(vals)
