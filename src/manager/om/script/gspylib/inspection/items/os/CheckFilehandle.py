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
import json
import multiprocessing
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode

# master
MASTER_INSTANCE = 0
# standby
STANDBY_INSTANCE = 1


class CheckFilehandle(BaseItem):
    def __init__(self):
        super(CheckFilehandle, self).__init__(self.__class__.__name__)
        self.Threshold_Warning = None

    def preCheck(self):
        super(CheckFilehandle, self).preCheck()
        if (not self.threshold.__contains__('Threshold_Warning')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The Threshold_Warning")
        if (not self.threshold['Threshold_Warning'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53014"]
                            % "The Threshold_Warning")
        self.Threshold_Warning = int(self.threshold['Threshold_Warning'])

    def doCheck(self):
        masterDNs = []
        slaveDNs = []
        masterDNhander = {}
        salveDNhander = {}
        overvalueDNs = []
        overmasterDNs = []
        flag = False
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        for DnInstance in nodeInfo.datanodes:
            if (DnInstance.instanceType == MASTER_INSTANCE):
                masterDNs.append(DnInstance)
            elif (DnInstance.instanceType == STANDBY_INSTANCE):
                slaveDNs.append(DnInstance)
        for dn in masterDNs:
            getpidcmd = "ps -ef| grep %s|grep -v 'grep'|awk '{print $2}'" \
                        % dn.datadir
            pid = SharedFuncs.runShellCmd(getpidcmd)
            getfilehander = "lsof | grep %s|wc -l" % pid
            filehander = SharedFuncs.runShellCmd(getfilehander)
            instanceName = "dn_%s" % (dn.instanceId)
            if (not filehander.isdigit()):
                num = filehander.splitlines()
                filehander = num[-1]
            if (not filehander.isdigit()):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53018"]
                                % (pid, getfilehander))
            masterDNhander[instanceName] = filehander
        for dn in slaveDNs:
            getpidcmd = "ps -ef| grep '%s'|grep -v 'grep'|awk '{print $2}'" \
                        % dn.datadir
            pid = SharedFuncs.runShellCmd(getpidcmd)
            getfilehander = "lsof | grep %s|wc -l" % pid
            filehander = SharedFuncs.runShellCmd(getfilehander)
            instanceName = "dn_%s" % (dn.instanceId)
            if (not filehander.isdigit()):
                num = filehander.splitlines()
                filehander = num[-1]
            if (not filehander.isdigit()):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53018"]
                                % (pid, getfilehander))
            salveDNhander[instanceName] = filehander
        for key, value in (masterDNhander.items()):
            if (int(value) > self.Threshold_Warning):
                overvalueDNs.append(key)
        for key, value in (salveDNhander.items()):
            if (int(value) > self.Threshold_Warning):
                overvalueDNs.append(key)
        for key, value in salveDNhander.items():
            for mkey, mastervalue in masterDNhander.items():
                if (int(value) > int(mastervalue)):
                    overmasterDNs.append(key)
                    flag = True
        if (overvalueDNs and flag):
            self.result.val = "Some slave database node open more file " \
                              "hander than master database node %s;" \
                              "Some gaussdb process open file handler over" \
                              " %s:\n%s" % ("\n".join(overmasterDNs),
                                            self.Threshold_Warning,
                                            "\n".join(overvalueDNs))
            self.result.rst = ResultStatus.WARNING
        elif (overvalueDNs):
            self.result.val = "Some gaussdb process open file handler" \
                              " over %s:\n%s" % (
                                  self.Threshold_Warning,
                                  "\n".join(overvalueDNs))
            self.result.rst = ResultStatus.WARNING
        elif (flag):
            self.result.val = "There is some slave database node open " \
                              "more file hander than master database node" \
                              " %s" % "\n".join(overmasterDNs)
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.val = "File hander check pass"
            self.result.rst = ResultStatus.OK
