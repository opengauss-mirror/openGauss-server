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

g_max = {}


class CheckGUCValue(BaseItem):
    def __init__(self):
        super(CheckGUCValue, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global g_max
        sqlcmd = "show max_connections;"
        self.result.raw = sqlcmd
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        max_connections = int(output.strip())
        g_max['conn'] = max_connections
        sqlcmd = "show max_prepared_transactions;"
        self.result.raw += sqlcmd
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        max_prepared_transactions = int(output.strip())
        g_max['pre'] = max_prepared_transactions
        sqlcmd = "show max_locks_per_transaction;"
        self.result.raw += sqlcmd
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        max_locks_per_transaction = int(output.strip())
        g_max['locks'] = max_locks_per_transaction
        max_value = int(max_locks_per_transaction) * (
                int(max_connections) + int(max_prepared_transactions))
        g_max['value'] = max_value
        self.result.val = "max_locks_per_transaction[%d] * (max_connections[" \
                          "%d] + max_prepared_transactions[%d]) = %d" % (
                              max_locks_per_transaction, max_connections,
                              max_prepared_transactions,
                              max_value)
        if (int(max_value) < int(1000000)):
            self.result.rst = ResultStatus.NG
            self.result.val += " Must be lager than 1000000"
        else:
            self.result.rst = ResultStatus.OK

    def doSet(self):
        if (g_max['pre'] > 1000):
            locksTransaction = int(
                1000000 // (g_max['pre'] + g_max['conn'])) + 1
            cmd = "gs_guc set -N all -I all -c " \
                  "'max_locks_per_transaction=%d'" % locksTransaction
            SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        else:
            cmd = "gs_guc set -N all -I all -c " \
                  "'max_locks_per_transaction=512' -c 'max_connections=1000'" \
                  " -c 'max_prepared_transactions = 1000'"
            SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
            cmd = "gs_guc set -N all -I all -c " \
                  "'max_locks_per_transaction=512' -c 'max_connections=1000'" \
                  " -c 'max_prepared_transactions = 1000'"
            SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        self.result.val = "Set GUCValue successfully."
