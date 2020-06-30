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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckKeyProAdj(BaseItem):
    def __init__(self):
        super(CheckKeyProAdj, self).__init__(self.__class__.__name__)

    def doCheck(self):
        procadj = {}
        result = ""
        prolist = ['om_monitor', 'cm_agent', 'gaussdb', 'cm_server', 'gtm',
                   'etcd']
        gausshome = self.cluster.appPath
        gaussdbpath = os.path.join(gausshome, "bin/gaussdb")
        for process in prolist:
            if (process == 'gaussdb'):
                getpidcmd = "ps ux| grep '%s'|grep -v 'grep'|awk '{print " \
                            "$2}'" \
                            % gaussdbpath
            else:
                getpidcmd = "ps ux| grep '%s'|grep -v 'grep'|awk '{print " \
                            "$2}'" \
                            % process
            pids = SharedFuncs.runShellCmd(getpidcmd)
            for pid in pids.splitlines():
                getAdjcmd = "cat /proc/%s/oom_adj" % pid
                adjValue = SharedFuncs.runShellCmd(getAdjcmd)
                if (int(adjValue) < 0):
                    tmpkey = "%s_%s" % (process, pid)
                    procadj[tmpkey] = adjValue
        if (procadj):
            self.result.rst = ResultStatus.NG
            for key, value in procadj.items():
                result += "%s : %s \n" % (key, value)
            self.result.val = "There are processes omm_adj value " \
                              "less than 0 \n%s" % (result)
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "All key processes omm_adj value" \
                              " are not less than 0"
