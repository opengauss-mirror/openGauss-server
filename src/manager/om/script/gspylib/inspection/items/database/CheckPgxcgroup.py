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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode


class CheckPgxcgroup(BaseItem):
    def __init__(self):
        super(CheckPgxcgroup, self).__init__(self.__class__.__name__)
        self.version = None

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__('version')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "version")
        self.version = self.threshold['version']

    def doCheck(self):
        if (self.version == "V1R7C10"):
            sqlcmd = "select count(group_name) from pgxc_group " \
                     "where in_redistribution='true' OR in_redistribution='y';"
        else:
            sqlcmd = "select count(group_name) " \
                     "from pgxc_group " \
                     "where in_redistribution='y' OR in_redistribution='t';"
        self.result.raw = sqlcmd
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)

        if (output != '0'):
            self.result.rst = ResultStatus.NG
            self.result.val = "Cluster not completed redistribution."
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The cluster has been redistributed."
