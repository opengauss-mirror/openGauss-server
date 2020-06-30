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

from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsservice import g_service


class CheckSshdService(BaseItem):
    def __init__(self):
        super(CheckSshdService, self).__init__(self.__class__.__name__)

    def doCheck(self):
        (status, output) = g_service.manageOSService('sshd', 'status')
        self.result.raw = output
        if (status == 0 and output.find('running')):
            self.result.rst = ResultStatus.OK
            self.result.val = "The sshd service is normal."
        else:
            self.result.val = "There is no sshd service."
            self.result.rst = ResultStatus.NG
