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
import platform
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsplatform import g_Platform


class CheckOSVer(BaseItem):
    def __init__(self):
        super(CheckOSVer, self).__init__(self.__class__.__name__)

    def doCheck(self):
        (distName, version) = g_Platform.getCurrentPlatForm()
        bits, linkage = platform.architecture()
        self.result.val = "The current OS is %s %s %s" % (
            distName, version, bits)
        if (distName in ("redhat", "centos")):
            if (version[0:3] in (
                    "6.4", "6.5", "6.6", "6.7", "6.8", "6.9", "7.0", "7.1",
                    "7.2",
                    "7.3", "7.4", "7.5", "7.6") and
                    bits == "64bit"):
                self.result.rst = ResultStatus.OK
                self.result.val = "The current OS is %s %s %s." % (
                    distName, version[0:3], bits)
            else:
                self.result.rst = ResultStatus.NG
        elif (distName == "euleros" and version in (
                "2.0", "2.3") and bits == "64bit"):
            self.result.rst = ResultStatus.OK
            self.result.val = "The current OS is EULER %s 64bit." % version
        elif (distName == "suse" and version in (
                "11.1", "11.2", "11.3", "11.4", "12.0", "12.1", "12.2",
                "12.3") and bits == "64bit"):
            self.result.rst = ResultStatus.OK
            self.result.val = "The current OS is SuSE %s 64bit." % version
        elif distName == "openeuler":
            self.result.rst = ResultStatus.OK
            self.result.val = "The current OS is openEuler %s." % version
        else:
            self.result.rst = ResultStatus.NG
            self.result.val = "The current OS[%s %s] " \
                              "does not meet the requirements." % (
                                  distName, version)

    def postAnalysis(self, itemResult, category="", name=""):
        errors = []
        for i in itemResult.getLocalItems():
            if (i.rst == ResultStatus.NG):
                errors.append("%s: %s" % (i.host, i.val))
        if len(errors) > 0:
            itemResult.rst = ResultStatus.NG
            itemResult.analysis = "\n".join(errors)
            return itemResult

        analysis = ""
        VerGroupDisk = {'RedHat6': [], 'RedHat7': [], 'Euler': [],
                        'SuSE11SP1': [], 'SuSE11SP234': [], 'SuSE12': [],
                        'openEuler': []}
        for v in itemResult.getLocalItems():
            analysis += "%s: %s\n" % (v.host, v.val)
            verInfo = v.val.strip().split(' ')[4:]
            if verInfo[0] in ("redhat", "centos"):
                if (verInfo[1][0:3] in (
                        "6.4", "6.5", "6.6", "6.7", "6.8", "6.9")):
                    VerGroupDisk['RedHat6'].append(verInfo)
                else:
                    VerGroupDisk['RedHat7'].append(verInfo)
            elif verInfo[0] == "euleros":
                VerGroupDisk['Euler'].append(verInfo)
            elif verInfo[0] == "openEuler":
                VerGroupDisk['openEuler'].append(verInfo)
            elif verInfo[0] == "SuSE":
                if verInfo[1] == "11.1":
                    VerGroupDisk['SuSE11SP1'].append(verInfo)
                elif verInfo[1] in ("11.2", "11.3", "11.4"):
                    VerGroupDisk['SuSE11SP234'].append(verInfo)
                else:
                    VerGroupDisk['SuSE12'].append(verInfo)
        currentVerGroup = []
        for verGroup in VerGroupDisk.keys():
            if len(VerGroupDisk[verGroup]) != 0:
                currentVerGroup.append(verGroup)
        if len(currentVerGroup) > 1:
            itemResult.rst = ResultStatus.NG
        else:
            itemResult.rst = ResultStatus.OK
        itemResult.analysis = analysis

        return itemResult
