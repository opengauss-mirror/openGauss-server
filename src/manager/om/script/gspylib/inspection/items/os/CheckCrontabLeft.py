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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsplatform import g_Platform
from gspylib.common.ErrorCode import ErrorCode


class CheckCrontabLeft(BaseItem):
    def __init__(self):
        super(CheckCrontabLeft, self).__init__(self.__class__.__name__)
        self.crontabUser = None

    def preCheck(self):
        super(CheckCrontabLeft, self).preCheck()
        if not "crontabUser" in self.threshold.keys():
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "Threshold crontabUser")
        self.crontabUser = self.threshold['crontabUser']

    def doCheck(self):
        parRes = ""
        cmd = g_Platform.getAllCrontabCmd()
        allCrontab = SharedFuncs.runShellCmd(cmd, self.user)
        for crontabService in allCrontab.split('\n'):
            if crontabService.find('om_monitor') >= 0:
                parRes = "Gauss process om_monitor remains in crontab. " \
                         "please delete this gauss info."
                self.result.raw += "%s\n" % crontabService
        if parRes:
            self.result.rst = ResultStatus.NG
            self.result.val = parRes
        else:
            self.result.rst = ResultStatus.OK

    def doSet(self):
        if os.getuid == 0:
            cmd = "crontab -l -u '%s'" % self.crontabUser
        else:
            cmd = "crontab -l"
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0 or output.find('om_monitor') < 0:
            self.result.val = "No gauss process in crontab.\n"
            return

        tmpCrondFileName = "gauss_crond_tmp"
        tmpCrondFile = os.path.join(self.tmpPath, tmpCrondFileName)
        try:
            SharedFuncs.createFile(tmpCrondFile, self.tmpPath)
            SharedFuncs.writeFile(tmpCrondFile, output, self.tmpPath)
            cmd = "sed -i '/om_monitor/d' %s" % tmpCrondFile
            SharedFuncs.runShellCmd(cmd)
            cmd = "crontab %s " % tmpCrondFile
            if os.getuid == 0:
                cmd = "su - %s '%s'" % (self.crontabUser, cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.result.val = "Failed to cleaned om_monitor in crontab." \
                                  " Error: %s\n" % output + "The cmd is %s " \
                                  % cmd
            else:
                self.result.val = "Successfully to cleaned om_monitor " \
                                  "in crontab.\n"
            SharedFuncs.cleanFile(tmpCrondFile)
        except Exception as e:
            if os.path.exists(tmpCrondFile):
                SharedFuncs.cleanFile(tmpCrondFile)
            raise Exception(str(e))

