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


class CheckIdleSession(BaseItem):
    def __init__(self):
        super(CheckIdleSession, self).__init__(self.__class__.__name__)

    def doCheck(self):
        dbNode = self.cluster.getDbNodeByName(self.host)
        sqlcmd = "select pid, query_id, application_name, query_start, " \
                 "state, " \
                 "query from pg_stat_activity where state <> 'idle' and " \
                 " application_name not in ('JobScheduler', " \
                 "'WorkloadManager', " \
                 "'WLMArbiter', 'workload', 'WorkloadMonitor', 'Snapshot', " \
                 "'PercentileJob') and " \
                 "query_id <> 0 and query not like '%pg_stat_activity%';"
        self.result.raw = sqlcmd
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        if (output != ''):
            self.result.rst = ResultStatus.NG
            self.result.val = output
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "No idle process."
