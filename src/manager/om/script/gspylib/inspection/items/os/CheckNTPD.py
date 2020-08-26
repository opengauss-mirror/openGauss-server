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
import re
from datetime import datetime, timedelta
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsplatform import g_Platform
from gspylib.os.gsOSlib import g_OSlib

DEFAULT_INTERVAL = 300


class ntp:
    def __init__(self):
        """
        function : Init class ntp
        input  : NA
        output : NA
        """
        self.running = False
        self.hosts = set()
        self.currentTime = ""
        self.errorMsg = None


class CheckNTPD(BaseItem):
    def __init__(self):
        super(CheckNTPD, self).__init__(self.__class__.__name__)

    def collectNtpd(self):
        data = ntp()
        try:
            p = subprocess.Popen(["/usr/sbin/ntpq",  "-p"], shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            result = p.communicate()
            data.errorMsg = result[1].decode().strip()
        except Exception as e:
            data.errorMsg = str(e)
            return data
        if not p.returncode:
            startHosts = False
            for line in result[0].decode().splitlines():
                if startHosts:
                    words = line.split()
                    if len(words) < 2:
                        continue
                    host = words[0].strip()
                    if host.startswith("*"):
                        host = host.lstrip("*")
                    data.hosts.add(host)
                else:
                    if re.search("======", line):
                        startHosts = True
        pidList = g_OSlib.getProcess('ntpd')
        for line in pidList:
            if (line.strip().isdigit()):
                data.running = True
        return data

    def doCheck(self):
        data = self.collectNtpd()
        data.currentTime = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not data.running:
            self.result.rst = ResultStatus.NG
            self.result.val = "NTPD service is not running, %s" \
                              % data.currentTime
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "NTPD service is running, %s" % data.currentTime
        self.result.raw = data.errorMsg

    def postAnalysis(self, itemResult, category="", name=""):
        errors = []
        for i in itemResult.getLocalItems():
            if i.rst == ResultStatus.NG :
                errors.append("%s: %s" % (i.host, i.val))
        if len(errors) > 0:
            itemResult.rst = ResultStatus.NG
            itemResult.analysis = "\n".join(errors)
            return itemResult
        keyStr = itemResult.getLocalItems()[0].val.strip().split(',')[
            1].strip()
        baseTime = datetime.strptime(keyStr, "%Y-%m-%d %H:%M:%S")
        startTime = baseTime
        endTime = baseTime

        rst = ResultStatus.OK
        analysis = ""
        for v in itemResult.getLocalItems():
            analysis += "%s: %s\n" % (v.host, v.val)
            tmpStr = v.val.strip().split(',')[1].strip()
            tmpTime = datetime.strptime(tmpStr, "%Y-%m-%d %H:%M:%S")
            if (tmpTime < startTime):
                startTime = tmpTime
            if (tmpTime > endTime):
                endTime = tmpTime

        if (endTime > (startTime + timedelta(seconds=DEFAULT_INTERVAL))):
            rst = ResultStatus.NG
            analysis = "Time difference between nodes more than 5 " \
                       "minute:\n%s" \
                       % analysis
        itemResult.rst = rst
        itemResult.analysis = analysis

        return itemResult
