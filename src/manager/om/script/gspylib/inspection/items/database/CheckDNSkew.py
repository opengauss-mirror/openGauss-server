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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file


class CheckDNSkew(BaseItem):
    def __init__(self):
        super(CheckDNSkew, self).__init__(self.__class__.__name__)

    def doCheck(self):
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        maxusage = None
        minusage = None
        usagedic = {}
        val = ""
        masterDnList = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        for DnInstance in nodeInfo.datanodes:
            if (DnInstance.instanceId in masterDnList):
                datadir = os.path.join(DnInstance.datadir, "base")
                output = g_file.getDirSize(datadir, "m")
                output = output.split()[0][:-1]
                if (not output.isdigit()):
                    raise Exception(ErrorCode.GAUSS_504["GAUSS_50412"]
                                    % (DnInstance.instanceId))
                if (not maxusage or int(maxusage) < int(output)):
                    maxusage = int(output)
                if (not minusage or int(minusage) > int(output)):
                    minusage = int(output)
                usagedic[DnInstance.instanceId] = output
                val += "\ndn %s: vol %sm" % (DnInstance.instanceId, output)
        if (not usagedic):
            self.result.rst = ResultStatus.NA
            self.result.val = "No master database node in this host"
        else:
            if (maxusage > minusage * 1.05):
                self.result.rst = ResultStatus.NG
                self.result.val = "The result is not ok:\n%s" % val
                self.result.raw = json.dumps(usagedic)
            else:
                self.result.rst = ResultStatus.OK
                self.result.val = "Data distributed well in local host"
                self.result.raw = json.dumps(usagedic)

    def postAnalysis(self, itemResult):
        maxusage = None
        minusage = None
        val = "The result is not ok"
        for v in itemResult.getLocalItems():
            try:
                tmpdic = json.loads(v.raw)
                for key, value in tmpdic.items():
                    val += "\ndn %s: vol %sM" % (key, value)
                    if (not maxusage or int(maxusage) < int(value)):
                        maxusage = int(value)
                    if (not minusage or int(minusage) > int(value)):
                        minusage = int(value)
            except Exception as err:
                val += str(err)
        if (maxusage and minusage):
            if (maxusage > minusage * 1.05):
                itemResult.rst = ResultStatus.NG
                itemResult.analysis = val
            else:
                itemResult.rst = ResultStatus.OK
                itemResult.analysis = "Data distributed well in all dns"
        else:
            itemResult.rst = ResultStatus.NA
            itemResult.analysis = "No master database node in this cluster"
        return itemResult
