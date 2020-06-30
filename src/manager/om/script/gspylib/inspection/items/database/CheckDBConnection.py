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


class CheckDBConnection(BaseItem):
    def __init__(self):
        super(CheckDBConnection, self).__init__(self.__class__.__name__)

    def doCheck(self):
        cmd = "gs_om -t status"
        output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        if (output.find("Normal") < 0 and output.find("Degraded") < 0):
            self.result.rst = ResultStatus.NG
            self.result.val = "The database can not be connected."
            return
        instanceList = []
        AbnormalInst = []
        primaryDnList = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        localDnList = nodeInfo.datanodes
        for dn in localDnList:
            if (dn.instanceId in primaryDnList):
                instanceList.append(dn)
        sqlcmd = "select pg_sleep(1);"
        for instance in instanceList:
            cmd = "gsql -m -d postgres -p %s -c '%s'" % (instance.port, sqlcmd)
            if (self.mpprcFile):
                cmd = "source '%s' && %s" % (self.mpprcFile, cmd)
            if (os.getuid() == 0):
                cmd = "su - %s -c \"%s\" " % (self.user, cmd)
            self.result.raw += "\n%s" % cmd
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 or output.find("connect to server failed") > 0):
                AbnormalInst.append(instance.instanceId)
                self.result.val += "The install %s can not be connected.\n" \
                                   % instance.instanceId
                self.result.raw += "\nError: %s" % output
        if AbnormalInst:
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The database connection is normal."
