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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1


class CheckMaxAsyIOrequests(BaseItem):
    def __init__(self):
        super(CheckMaxAsyIOrequests, self).__init__(self.__class__.__name__)

    def collectAsynchronousIORequest(self):
        result = []
        request = g_file.readFile("/proc/sys/fs/aio-max-nr")[0]
        result.append(request.strip())
        return result

    def getClusterInstancenum(self):
        cnnum = 0
        dnnum = 0
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        for i in nodeInfo.coordinators:
            if i.datadir != "":
                cnnum += 1

        for i in nodeInfo.datanodes:
            if (i.instanceType == MASTER_INSTANCE):
                dnnum += 1
            if (i.instanceType == STANDBY_INSTANCE):
                dnnum += 1

        return (dnnum + cnnum) * 1048576

    def doCheck(self):
        expectedScheduler = "104857600"
        flag = True
        resultStr = ""
        instancenum = 0

        if (self.cluster):
            instancenum = self.getClusterInstancenum()

        data = self.collectAsynchronousIORequest()

        if len(data) == 0:
            flag = False
            resultStr += "Not find AsynchronousIORequest file.\n"
            if (instancenum > expectedScheduler):
                resultStr += "Asy IO requests must be greater than %s.\n" \
                             % instancenum
            else:
                resultStr += "Asy IO requests must be greater than %s.\n" \
                             % expectedScheduler
        else:
            for i in iter(data):
                request = i
                if (int(request) < int(instancenum) and int(
                        expectedScheduler) < int(instancenum)):
                    flag = False
                    resultStr += "Asy IO requests %s  expectedScheduler " \
                                 "%s.\n" \
                                 % (
                                     request, instancenum)
                elif (int(request) < int(expectedScheduler) and int(
                        instancenum) < int(expectedScheduler)):
                    flag = False
                    resultStr += "Asy IO requests %s  expectedScheduler " \
                                 "%s.\n" \
                                 % (
                                     request, expectedScheduler)
                else:
                    resultStr += "Asy IO requests is %s\n" % request
        self.result.val = resultStr
        if flag:
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.WARNING

    def doSet(self):
        resultStr = ""
        cmd = "echo 104857600 > /proc/sys/fs/aio-max-nr"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr = "Failed to set Asy IO requests.\nError : %s." % \
                        output + "The cmd is %s " % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set Asy IO requests successfully."
