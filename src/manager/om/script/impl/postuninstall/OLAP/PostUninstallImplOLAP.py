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
import sys

sys.path.append(sys.path[0] + "/../")

from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from impl.postuninstall.PostUninstallImpl import PostUninstallImpl

#############################################################################
# Global variables
#############################################################################
# action name
ACTION_CLEAN_VIRTUALIP = "clean_virtualIp"


class PostUninstallImplOLAP(PostUninstallImpl):
    """
    init the command options
    input : NA
    output: NA
    """

    def __init__(self, GaussPost):
        self.xmlFile = GaussPost.xmlFile
        self.logFile = GaussPost.logFile
        self.deleteUser = GaussPost.deleteUser
        self.deleteGroup = GaussPost.deleteGroup
        self.nodeList = GaussPost.nodeList
        self.localLog = GaussPost.localLog
        self.user = GaussPost.user
        self.group = GaussPost.group
        self.mpprcFile = GaussPost.mpprcFile
        self.clusterToolPath = GaussPost.clusterToolPath
        self.localMode = GaussPost.localMode
        self.logger = GaussPost.logger
        self.sshTool = GaussPost.sshTool
        self.clusterInfo = GaussPost.clusterInfo
        self.clean_gphome = GaussPost.clean_gphome
        self.clean_host = GaussPost.clean_host
        self.sshpwd = GaussPost.sshpwd
        self.userHome = GaussPost.userHome

