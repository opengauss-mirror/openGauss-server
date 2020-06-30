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
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsservice import g_service


class CheckCrondService(BaseItem):
    def __init__(self):
        super(CheckCrondService, self).__init__(self.__class__.__name__)

    def doCheck(self):
        (status, crondInfo) = g_service.manageOSService('crond', 'status')
        self.result.raw = crondInfo
        # Resolve and outputs the execution results of each node
        if (status != 0 or crondInfo.find('running') < 0):
            self.result.val = "There is no cron service."
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The cron service is normal."

    def doSet(self):
        if SharedFuncs.isSupportSystemOs():
            cmd = "/sbin/service crond start"
        else:
            cmd = "/sbin/service cron start"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val = "Failed to started crond service. " \
                              "Error: %s\n" % output + "The cmd is %s " % cmd
        else:
            self.result.val = "Successfully started the crond service.\n"
