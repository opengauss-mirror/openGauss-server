#-*- coding:utf-8 -*-
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

import sys

sys.path.append(sys.path[0] + "/../../../")
from impl.upgrade.UpgradeImpl import UpgradeImpl


#############################################################################
# Global variables
#############################################################################


class UpgradeImplOLAP(UpgradeImpl):
    """
    The class is used to do perform upgrade
    """
    def __init__(self, upgrade):
        super(UpgradeImplOLAP, self).__init__(upgrade)
