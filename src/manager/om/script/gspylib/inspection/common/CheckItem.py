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
from gspylib.inspection.common import SharedFuncs

import json
import imp
import types
from abc import abstractmethod
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.inspection.common.Log import LoggerFactory
from gspylib.inspection.common.CheckResult import LocalItemResult, \
    ResultStatus
from gspylib.inspection.common.Exception import CheckNAException


def defaultAnalysis(self, itemResult):
    # Get the item result information
    itemResult.standard = self.standard
    itemResult.suggestion = self.suggestion
    itemResult.category = self.category
    itemResult.title = self.title

    errors = []
    ngs = []
    warnings = []
    vals = {}
    for i in itemResult.getLocalItems():
        if (i.rst == ResultStatus.OK or i.rst == ResultStatus.NA):
            if (i.val):
                vals[i.host] = i.val
            continue
        elif (i.rst == ResultStatus.ERROR):
            errors.append("%s : %s" % (i.host, i.val))
        elif (i.rst == ResultStatus.WARNING):
            warnings.append("%s : %s" % (i.host, i.val))
        else:
            ngs.append("%s : %s" % (i.host, i.val))
    # Analysis results
    if (len(ngs) > 0 or len(errors) > 0 or len(warnings) > 0):
        rst = ResultStatus.WARNING
        if len(errors) > 0:
            rst = ResultStatus.ERROR
        elif len(ngs) > 0:
            rst = ResultStatus.NG
        itemResult.rst = rst
        itemResult.analysis = "\n".join(ngs + errors + warnings)
    else:
        itemResult.rst = ResultStatus.OK
        itemResult.analysis = ""

    analysisStrList = []
    nas, oks, ngs, warnings, errors = classifyItemResult(itemResult)
    total = len(oks) + len(ngs) + len(warnings) + len(errors)

    rst = ResultStatus.OK
    okMsg, okAnalysisList = countItemResult(oks)
    warningMsg, warningAnalysisList = countItemResult(warnings)
    failedMsg, failedAnalysisList = countItemResult(ngs)
    errorMsg, errorAnalysisList = countItemResult(errors)
    if (len(warnings) > 0):
        rst = ResultStatus.WARNING
    if (len(ngs) > 0):
        rst = ResultStatus.NG
    if (len(errors) > 0):
        rst = ResultStatus.ERROR
    countMsg = "The item run on %s nodes. %s%s%s%s" % (
        total, okMsg, warningMsg, failedMsg, errorMsg)
    analysisStrList.append(countMsg)
    if (errorAnalysisList):
        analysisStrList.extend(errorAnalysisList)
    if (failedAnalysisList):
        analysisStrList.extend(failedAnalysisList)
    if (warningAnalysisList):
        analysisStrList.extend(warningAnalysisList)
    if (itemResult.name == 'CheckSysTable'):
        value = [vals[key] for key in sorted(vals.keys())]
        analysisStrList.extend(value)
    itemResult.rst = rst
    itemResult.analysis = "\n".join(analysisStrList)
    return itemResult


def consistentAnalysis(self, itemResult):
    # check the rst in each node and make sure the var is consistence
    itemResult.standard = self.standard
    itemResult.suggestion = self.suggestion
    itemResult.category = self.category
    itemResult.title = self.title

    analysisStrList = []
    nas, oks, ngs, warnings, errors = classifyItemResult(itemResult)
    total = len(oks) + len(ngs) + len(warnings) + len(errors)

    # The item run on %s nodes. success: %s  warning: %s ng:%s error:%
    rst = ResultStatus.OK
    if (len(oks) == total):
        okMsg, okAnalysisList = countItemResult(oks, True)
    else:
        okMsg, okAnalysisList = countItemResult(oks)
    warningMsg, warningAnalysisList = countItemResult(warnings)
    failedMsg, failedAnalysisList = countItemResult(ngs)
    errorMsg, errorAnalysisList = countItemResult(errors)
    if (len(okAnalysisList) > 0):
        okMsg += " (consistent) " if (
                len(okAnalysisList) == 1) else " (not consistent) "

    if (len(warnings) > 0 and rst == ResultStatus.OK):
        rst = ResultStatus.WARNING
    if (len(okAnalysisList) > 1):
        rst = ResultStatus.NG
        if (itemResult.name in ["CheckDiskConfig", "CheckCpuCount",
                                "CheckMemInfo", "CheckStack",
                                "CheckKernelVer"]):
            rst = ResultStatus.WARNING
    if (len(ngs) > 0):
        rst = ResultStatus.NG
    if (len(errors) > 0):
        rst = ResultStatus.ERROR

    countMsg = "The item run on %s nodes. %s%s%s%s" % (
        total, okMsg, warningMsg, failedMsg, errorMsg)
    analysisStrList.append(countMsg)
    if (errorAnalysisList):
        analysisStrList.extend(errorAnalysisList)
    if (failedAnalysisList):
        analysisStrList.extend(failedAnalysisList)
    if (warningAnalysisList):
        analysisStrList.extend(warningAnalysisList)
    if (okAnalysisList):
        analysisStrList.extend(okAnalysisList)
    itemResult.rst = rst
    itemResult.analysis = "\n".join(analysisStrList)
    return itemResult


def getValsItems(vals):
    """

    :param vals:
    :return:
    """
    ret = {}
    for i_key, i_val in list(vals.items()):
        try:
            i_val = eval(i_val)
        except Exception:
            i_val = i_val
        if isinstance(i_val, dict):
            for j_key, j_val in list(i_val.items()):
                ret[j_key] = j_val

    return ret


def getCheckType(category):
    '''
    function : get check type
    input : category
    output : 1,2,3
    '''
    if not category:
        return 0
    if category == "cluster":
        return 1
    elif category == "database":
        return 3
    else:
        return 2


def classifyItemResult(itemResult):
    nas = []
    oks = []
    ngs = []
    wns = []
    ers = []
    # Summary results
    for i in itemResult.getLocalItems():
        if (i.rst == ResultStatus.OK):
            oks.append(i)
        if (i.rst == ResultStatus.NA):
            nas.append(i)
        if (i.rst == ResultStatus.NG):
            ngs.append(i)
        if (i.rst == ResultStatus.WARNING):
            wns.append(i)
        if (i.rst == ResultStatus.ERROR):
            ers.append(i)
    return (nas, oks, ngs, wns, ers)


def countItemResult(itemList, allNode=False):
    if (itemList is None or len(itemList) == 0):
        return ("", [])
    first = itemList[0]
    msgTitle = "default"
    if (first.rst == ResultStatus.WARNING):
        msgTitle = "warning"
    if (first.rst == ResultStatus.NG):
        msgTitle = "ng"
    if (first.rst == ResultStatus.ERROR):
        msgTitle = "error"
    if (first.rst == ResultStatus.OK):
        msgTitle = "success"
    countMsg = " %s: %s " % (msgTitle, len(itemList))

    defaultHosts = [first.host]
    diffs = []
    for i in itemList[1:]:
        if i.val == first.val:
            defaultHosts.append(i.host)
            continue
        else:
            diffs.append("The different[%s] value:\n%s" % (i.host, i.val))
    if (allNode):
        analysisStrList = [
            "The %s on all nodes value:\n%s" % (msgTitle, first.val)]
    else:
        analysisStrList = ["The %s%s value:\n%s" % (
            msgTitle, '[' + ",".join(defaultHosts) + ']', first.val)]
    if (len(diffs) > 0):
        analysisStrList.extend(diffs)
    return (countMsg, analysisStrList)


class BaseItem(object):
    '''
    base class of check item
    '''

    def __init__(self, name):
        '''
        Constructor
        '''
        self.name = name
        self.title = None
        self.set = False
        self.log = None
        self.suggestion = None
        self.standard = None
        self.threshold = {}
        self.category = 'other'
        self.permission = 'user'
        self.analysis = 'default'
        self.scope = 'all'
        self.cluster = None
        self.port = None
        self.user = None
        self.nodes = None
        self.mpprcFile = None
        self.thresholdDn = None
        self.context = None
        self.tmpPath = None
        self.outPath = None
        self.host = DefaultValue.GetHostIpOrName()
        self.result = LocalItemResult(name, self.host)
        self.routing = None
        self.skipSetItem = []
        self.ipAddr = None
        # self cluster name not only lc
        self.LCName = None
        self.ShrinkNodes = None

    @abstractmethod
    def preCheck(self):
        '''
        abstract precheck for check item
        '''
        pass

    @abstractmethod
    def doCheck(self):
        '''
        check script for each item
        '''
        pass

    @abstractmethod
    def postAnalysis(self, itemResult, category="", name=""):
        '''
        analysis the item result got from each node
        '''
        pass

    def initFrom(self, context):
        '''
        initialize the check item from context
        '''
        item = next(i for i in context.items if i['name'] == self.name)
        if item:
            self.title = self.__getLocaleAttr(item, 'title')
            self.suggestion = self.__getLocaleAttr(item, 'suggestion')
            self.standard = self.__getLocaleAttr(item, 'standard')
            if (item.__contains__('threshold')):
                self.category = item['category']
            if (item.__contains__('threshold')):
                self.threshold = item['threshold']
            # set pre check method
            self.setScope(item['scope'])
            # set post analysis method
            self.setAnalysis(item['analysis'])

        self.context = context
        self.cluster = context.cluster
        self.user = context.user
        self.nodes = context.nodes
        self.mpprcFile = context.mpprc
        self.result.checkID = context.checkID
        self.result.user = context.user
        self.tmpPath = context.tmpPath
        self.outPath = context.outPath
        self.set = context.set
        self.log = context.log
        self.routing = context.routing
        self.skipSetItem = context.skipSetItem
        self.__getLocalIP(context.nodes)
        self.LCName = context.LCName
        self.ShrinkNodes = context.ShrinkNodes
        if not context.thresholdDn:
            self.thresholdDn = 90
        else:
            self.thresholdDn = context.thresholdDn
        # new host without cluster installed
        if (not self.user):
            self.host = DefaultValue.GetHostIpOrName()
            self.result.host = DefaultValue.GetHostIpOrName()

    def __getLocalIP(self, nodeList):
        for node in nodeList:
            if (SharedFuncs.is_local_node(node) and SharedFuncs.validate_ipv4(
                    node)):
                self.ipAddr = node
                return

    def __getLocaleAttr(self, obj, attr, language='zh'):
        '''
        get attribute value for different language
        '''
        locAttr = str(attr) + '_' + language
        if (not obj.__contains__(locAttr) or obj[locAttr] == ""):
            return obj[str(attr) + '_' + 'zh']
        else:
            return obj[locAttr]

    def setScope(self, scope):
        # Choose execution node
        self.scope = scope
        # cn node to perform the check
        if (scope == 'cn'):
            self.preCheck = self.__cnPreCheck(self.preCheck)
        # Local implementation of the inspection
        elif (scope == 'local'):
            self.preCheck = self.__localPreCheck(self.preCheck)

    def setAnalysis(self, analysis):
        # Analyze the test results
        self.analysis = analysis
        # Consistency analysis for ap
        if (analysis == 'consistent'):
            self.postAnalysis = types.MethodType(consistentAnalysis, self)
        # Default analysis for ap
        elif (analysis == 'default'):
            self.postAnalysis = types.MethodType(defaultAnalysis, self)

    def runCheck(self, context, g_logger):
        '''
        main process for checking
        '''
        try:
            g_logger.debug("Start to run %s" % self.name)
            # initialization
            self.initFrom(context)
            self.preCheck()
            # Perform the inspection
            self.doCheck()
            if (self.set and (
                    self.result.rst == ResultStatus.NG
                    or self.result.rst == ResultStatus.WARNING)
                    and self.name not in self.skipSetItem):
                self.doSet()
                self.doCheck()
            g_logger.debug("Finish to run %s" % self.name)
        except CheckNAException:
            self.result.rst = ResultStatus.NA
        # An internal error occurred while executing code
        except Exception as e:
            self.result.rst = ResultStatus.ERROR
            self.result.val = str(e)
            g_logger.debug(
                "Exception occur when running %s:\n%s" % (self.name, str(e)))
        finally:
            # output result
            self.result.output(context.tmpPath)

    def __cnPreCheck(self, func):
        # cn Pre-check node
        def wrapper():
            if (not hasattr(self, 'cluster')):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53030"]
                                % "cluster attribute")
            if (not hasattr(self, 'host')):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53030"]
                                % "host attribute")
            if (not self.cluster):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53031"])
            dbNode = self.cluster.getDbNodeByName(self.host)
            # The specified node does not exist or is empty
            if (dbNode is None or dbNode == ""):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                                % "The dbNode")
            if self.cluster.isSingleInstCluster():
                masterDn = SharedFuncs.getMasterDnNum(self.user,
                                                      self.mpprcFile)
                if len(dbNode.datanodes) < 1 or dbNode.datanodes[
                    0].instanceId not in masterDn:
                    raise CheckNAException(
                        "The node does not contains materDn instance")
                self.port = dbNode.datanodes[0].port
            else:
                # The specified CN node does not exist
                if (len(dbNode.coordinators) == 0):
                    raise CheckNAException(
                        "The node does not contains cn instance")
                # get cn port
                self.port = dbNode.coordinators[0].port
                self.cntype = dbNode.coordinators[0].instanceType
            return func()

        return wrapper

    def __localPreCheck(self, func):
        def wrapper():
            return func()

        return wrapper


class CheckItemFactory(object):
    @staticmethod
    def createItem(name, path, scope='all', analysis='default'):
        mod = imp.load_source(name, path)
        clazz = getattr(mod, name)
        checker = clazz()
        # set pre check method
        checker.setScope(scope)
        # set post analysis method
        checker.setAnalysis(analysis)
        return checker

    @staticmethod
    def createFrom(name, path, context):
        mod = imp.load_source(name, path)
        clazz = getattr(mod, name)
        checker = clazz()
        checker.initFrom(context)
        return checker
