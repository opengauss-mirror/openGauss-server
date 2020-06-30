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
"""
This file is for Gauss version things.
"""

import os
import sys
import re

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode


class VersionInfo():
    """
    Info about current version
    """

    def __init__(self):
        pass

    # package version
    __PACKAGE_VERSION = ""
    # OM version string
    COMMON_VERSION = "Gauss200 OM VERSION"
    # It will be replaced with the product version, such as "Gauss200",
    # while being packaged by mpp_package.sh
    PRODUCT_NAME = "__GAUSS_PRODUCT_STRING__"
    PRODUCT_NAME_PACKAGE = "-".join(PRODUCT_NAME.split())
    __COMPATIBLE_VERSION = []
    COMMITID = ""

    @staticmethod
    def getPackageVersion():
        """
        function: Get the current version from version.cfg
        input : NA
        output: String
        """
        if (VersionInfo.__PACKAGE_VERSION != ""):
            return VersionInfo.__PACKAGE_VERSION
        # obtain version file
        versionFile = VersionInfo.get_version_file()
        version, number, commitid = VersionInfo.get_version_info(versionFile)
        # the 2 value is package version
        VersionInfo.__PACKAGE_VERSION = version
        return VersionInfo.__PACKAGE_VERSION

    @staticmethod
    def getCommitid():
        if VersionInfo.COMMITID != "":
            return VersionInfo.COMMITID
        versionFile = VersionInfo.get_version_file()
        version, number, commitid = VersionInfo.get_version_info(versionFile)
        # the 2 value is package version
        VersionInfo.COMMITID = commitid
        return VersionInfo.COMMITID

    @staticmethod
    def get_version_file():
        """
        function: Get version.cfg file
        input : NA
        output: String
        """
        # obtain version file
        dirName = os.path.dirname(os.path.realpath(__file__))
        versionFile = os.path.join(dirName, "./../../../", "version.cfg")
        versionFile = os.path.realpath(versionFile)
        if (not os.path.exists(versionFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % versionFile)
        if (not os.path.isfile(versionFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % versionFile)
        return versionFile

    @staticmethod
    def get_version_info(versionFile):

        # the infomation of versionFile like this:
        #     openGauss-1.0
        #     XX.0
        #     ae45cfgt
        if not os.path.exists(versionFile):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % versionFile)
        if not os.path.isfile(versionFile):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % versionFile)
        with open(versionFile, 'r') as fp:
            retLines = fp.readlines()
        if len(retLines) < 3:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50222"] % versionFile)

        version = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+').search(
            retLines[0].strip()).group()
        number = retLines[1].strip()
        commitId = retLines[2].strip()

        if version is None:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50222"] %
                            "version.cfg" + "Does not have version "
                                            "such as openGauss-1.0")
        try:
            float(number)
        except Exception as e:
            raise Exception(str(e) + ErrorCode.GAUSS_516["GAUSS_51628"]
                            % number)

        if not (commitId.isalnum() and len(commitId) == 8):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50222"] % versionFile
                            + " Commit id is wrong.")
        return version, number, commitId
