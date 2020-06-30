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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckOmmUserExist(BaseItem):
    def __init__(self):
        super(CheckOmmUserExist, self).__init__(self.__class__.__name__)

    def doCheck(self):
        cmd = "id omm"
        (status, output) = subprocess.getstatusoutput(cmd)
        self.result.raw = output
        if (output.lower().find('no such user') < 0):
            self.result.rst = ResultStatus.NG
            self.result.val = "User omm already exists. " \
                              "please delete this omm " \
                              "used by 'userdel -rf omm'."
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = output

    def doSet(self):
        cmd = "userdel -rf omm"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val += "Failed to delete omm user. Error:%s\n" % output
            self.result.val += "The cmd is %s " % cmd
        else:
            self.result.val += "Successfully deleted omm user.\n"
