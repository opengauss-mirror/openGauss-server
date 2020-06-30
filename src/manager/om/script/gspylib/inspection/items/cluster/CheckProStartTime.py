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
from datetime import datetime
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common.CheckResult import ResultStatus

monthdic = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


class CheckProStartTime(BaseItem):
    def __init__(self):
        super(CheckProStartTime, self).__init__(self.__class__.__name__)

    def doCheck(self):
        self.result.rst = ResultStatus.OK
        timelist = []
        gaussPro = "gaussdb"
        cmd = "ps -C %s -o lstart,args | grep -v grep | grep -v 'om_monitor'" \
              " 2>/dev/null" % gaussPro
        output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        for line in output.splitlines()[1:]:
            resultList = line.split()
            year = resultList[4]
            month = monthdic[resultList[1]]
            day = resultList[2]
            time = resultList[3]
            timestring = "%s-%s-%s %s" % (year, month, day, time)
            dattime = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S')
            timelist.append(dattime)
        if (timelist):
            mintime = timelist[0]
            maxtime = timelist[0]
        else:
            mintime = None
            maxtime = None
        for tmpdatetime in timelist:
            if (tmpdatetime < mintime):
                mintime = tmpdatetime
            elif (tmpdatetime > maxtime):
                maxtime = tmpdatetime
        if (maxtime and mintime):
            if (int((maxtime - mintime).days) > 0 or int(
                    (maxtime - mintime).seconds) > 300):
                self.result.rst = ResultStatus.WARNING
                self.result.val = output
            else:
                self.result.rst = ResultStatus.OK
                self.result.val = output

    def postAnalysis(self, itemResult):
        errors = []
        timedic = {}
        valdic = {}
        allhost = []
        nghost = []
        Mintime = None
        for v in itemResult.getLocalItems():
            output = v.val
            timelist = []
            for line in output.splitlines()[1:]:
                resultList = line.split()
                year = resultList[4]
                month = monthdic[resultList[1]]
                day = resultList[2]
                time = resultList[3]
                timestring = "%s-%s-%s %s" % (year, month, day, time)
                dattime = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S')
                timelist.append(dattime)
            if (timelist):
                mintime = timelist[0]
                maxtime = timelist[0]
            else:
                mintime = None
                maxtime = None
            for tmpdatetime in timelist:
                if (tmpdatetime < mintime):
                    mintime = tmpdatetime
                elif (tmpdatetime > maxtime):
                    maxtime = tmpdatetime
            timelist = []
            if (maxtime and mintime):
                timelist.append(mintime)
                timelist.append(maxtime)
                if (Mintime and Mintime < mintime):
                    pass
                else:
                    Mintime = mintime
            if (timelist):
                timedic[v.host] = timelist
                valdic[v.host] = output
                allhost.append(v.host)
        for host in allhost:
            hostmax = timedic[host][1]
            if (int((hostmax - Mintime).days) > 0 or int(
                    (hostmax - Mintime).seconds) > 300):
                if (host not in nghost):
                    nghost.append(host)

        if (nghost):
            itemResult.rst = ResultStatus.WARNING
            resultStr = ""
            for host in nghost:
                resultStr += "%s:\n%s\n" % (host, valdic[host])
            itemResult.analysis = resultStr
        else:
            itemResult.rst = ResultStatus.OK
            itemResult.analysis = "Basically ,all the gaussdb process" \
                                  " start at the same time"
        return itemResult
