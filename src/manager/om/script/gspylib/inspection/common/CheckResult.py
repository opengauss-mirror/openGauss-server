# -*- coding:utf-8 -*-
#############################################################################
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
#############################################################################

import os
import sys
import json
import time
import pwd
from gspylib.inspection.common import SharedFuncs
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common.Log import LoggerFactory

class GsCheckEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


class ResultStatus(object):
    OK = "OK"
    NA = "NA"
    WARNING = "WARNING"
    NG = "NG"
    ERROR = "ERROR"


class LocalItemResult(object):
    '''
    the check result running on one host
    '''

    def __init__(self, name, host):
        self.name = name
        self.host = host
        self.raw = ""
        self.rst = ResultStatus.NA
        self.val = ""
        self.checkID = None
        self.user = None

    def output(self, outPath):
        u"""
[HOST]  {host}
[NAM]   {name}
[RST]   {rst}
[VAL]
{val}
[RAW]
{raw}
        """

        val = self.val if self.val else ""
        raw = self.raw if self.raw else ""
        try:
            content = self.output.__doc__.format(name=self.name, rst=self.rst,
                                                 host=self.host, val=val,
                                                 raw=raw)
        except Exception:
            content = self.output.__doc__.encode('utf-8').format(
                name=self.name, rst=self.rst, host=self.host, val=val,
                raw=raw).decode('utf-8', 'ignore')
        fileName = "%s_%s_%s.out" % (self.name, self.host, self.checkID)
        # output the result to local path
        SharedFuncs.writeFile(fileName, content, outPath,
                              DefaultValue.KEY_FILE_MODE, self.user)


class ItemResult(object):
    def __init__(self, name):
        self.name = name
        self._items = []
        self.rst = ResultStatus.NA
        self.standard = ""
        self.suggestion = ""
        self.category = 'other'
        self.analysis = ""

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, idx):
        return self._items[idx]

    def append(self, val):
        self._items.append(val)

    def formatOutput(self, detail=False):
        result = u"{name:.<25}...............{rst:.>6}".format(name=self.name,
                                                               rst=self.rst)
        result += u"\r\n%s\r\n" % self.analysis
        return result

    def getLocalItems(self):
        return self._items

    @staticmethod
    def parse(output):
        itemResult = None
        localItemResult = None
        host = None
        idx = 0
        for line in output.splitlines():
            idx += 1
            if (idx == len(
                    output.splitlines()) and localItemResult is not None):
                itemResult.append(localItemResult)
            current = line.strip()
            if (not current):
                continue
            if (current.startswith('[HOST]')):
                host = current.split()[1].strip()
            if (current.startswith('[NAM]')):
                name = current.split()[1].strip()
                if (itemResult is None):
                    itemResult = ItemResult(name)
                if (localItemResult is not None):
                    itemResult.append(localItemResult)
                localItemResult = LocalItemResult(current.split()[1].strip(),
                                                  host)
            if (current.startswith('[RST]')):
                localItemResult.rst = current.split()[1].strip()
            if (current.startswith('[VAL]')):
                localItemResult.val = ItemResult.__parseMultiLine(
                    output.splitlines()[idx:])
            if (current.startswith('[RAW]')):
                localItemResult.raw = ItemResult.__parseMultiLine(
                    output.splitlines()[idx:])
        return itemResult

    @staticmethod
    def __parseMultiLine(lines):
        vals = []
        starter = ('[HOST]', '[NAM]', '[RST]', '[VAL]', '[RAW]')
        for line in lines:
            current = line.strip()
            if (current.startswith(starter)):
                break
            else:
                vals.append(current)
        return "\n".join(vals)


class CheckResult(object):
    def __init__(self):
        self._items = []

    def __iter__(self):
        return iter(self._items)

    def __getitem__(self, idx):
        return self._items[idx]

    def append(self, val):
        self._items.append(val)

    def outputStatistic(self):
        ok = 0
        warning = 0
        ng = 0
        error = 0
        for i in self._items:
            if (i.rst == ResultStatus.ERROR):
                error += 1
            elif (i.rst == ResultStatus.NG):
                ng += 1
            elif (i.rst == ResultStatus.WARNING):
                warning += 1
            else:
                ok += 1
        okMsg = " Success:%s " % ok if ok > 0 else ""
        warningMsg = " Warning:%s " % warning if warning > 0 else ""
        ngMsg = " NG:%s " % ng if ng > 0 else ""
        errorMsg = " Error:%s " % error if error > 0 else ""
        result = ""
        result += "Failed." if (ng + error) > 0 else "Success."
        result += "\tAll check items run completed. Total:%s  %s %s %s %s" % (
            ok + warning + ng + error, okMsg, warningMsg, ngMsg, errorMsg)
        return result

    def outputRaw(self):
        u"""
{date} [NAM] {name}
{date} [STD] {standard}
{date} [RST] {rst}
{val}
{date} [RAW]
{raw}
        """

        result = ""
        for i in self._items:
            for j in i._items:
                t = time.localtime(time.time())
                dateString = time.strftime("%Y-%m-%d %H:%M:%S", t)
                rst = j.rst
                if (j.rst == ResultStatus.NA):
                    rst = "NONE"
                elif (
                        j.rst == ResultStatus.WARNING
                        or j.rst == ResultStatus.ERROR):
                    rst = "NG"
                result += self.outputRaw.__doc__.format(date=dateString,
                                                        name=j.name,
                                                        standard=i.standard,
                                                        rst=rst,
                                                        val=j.val, raw=j.raw)
                result += "\r\n"
        return result

    def outputResult(self):
        result = ""
        for i in self._items:
            result += i.formatOutput()
            result += "\r\n"
        result += self.outputStatistic()
        return result

    def outputJson(self):
        resultDic = {}
        for itemResult in self._items:
            resultDic['name'] = itemResult.name
            resultDic['category'] = itemResult.category
            resultDic['std'] = itemResult.standard.decode('utf-8', 'ignore')
            resultDic['rst'] = itemResult.rst
            resultDic['analysis'] = itemResult.analysis
            resultDic['suggestion'] = itemResult.suggestion
            localList = []
            for localitem in itemResult:
                local = {}
                local['host'] = localitem.host
                local['rstd'] = localitem.val
                local['raw'] = localitem.raw
                localList.append(local)
            resultDic['hosts'] = localList
        return json.dumps(resultDic, cls=GsCheckEncoder, indent=2)
