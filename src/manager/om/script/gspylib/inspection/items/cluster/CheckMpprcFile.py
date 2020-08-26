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

import os
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckMpprcFile(BaseItem):
    def __init__(self):
        super(CheckMpprcFile, self).__init__(self.__class__.__name__)

    def doCheck(self):
        self.result.rst = ResultStatus.NG
        self.result.val = "There are illegal characters in mpprc file"
        appPath = self.cluster.appPath
        mpprcFile = self.mpprcFile
        bashfile = "/home/%s/.bashrc" % self.user
        if (mpprcFile == "" or not mpprcFile or mpprcFile == "/etc/profile"
                or mpprcFile == "~/.bashrc" or mpprcFile == bashfile
                or not os.path.exists(mpprcFile)):
            self.result.rst = ResultStatus.NG
            self.result.val = "There is no mpprc file"
            return
        try:
            with open(mpprcFile, 'r') as fp:
                env_list = fp.readlines()
            while '' in env_list:
                env_list.remove('')
            # get ec content
            ec_content = "if [ -f '%s/utilslib/env_ec' ] &&" \
                         " [ `id -u` -ne 0 ];" \
                         " then source '%s/utilslib/env_ec'; fi " \
                         % (appPath, appPath)
            ec_content_old = "if [ -f '%s/utilslib/env_ec' ] ;" \
                             " then source '%s/utilslib/env_ec'; fi " \
                             % (appPath, appPath)
            # remove ec content from list
            if ec_content in env_list:
                env_list.remove(ec_content)
            if ec_content_old in env_list:
                env_list.remove(ec_content_old)
            # white elements
            list_white = ["ELK_CONFIG_DIR", "ELK_SYSTEM_TABLESPACE",
                          "MPPDB_ENV_SEPARATE_PATH", "GPHOME", "PATH",
                          "LD_LIBRARY_PATH", "PYTHONPATH",
                          "GAUSS_WARNING_TYPE", "GAUSSHOME", "PATH",
                          "LD_LIBRARY_PATH",
                          "S3_CLIENT_CRT_FILE", "GAUSS_VERSION", "PGHOST",
                          "GS_CLUSTER_NAME", "GAUSSLOG",
                          "GAUSS_ENV", "KRB5_CONFIG", "PGKRBSRVNAME",
                          "KRBHOSTNAME", "ETCD_UNSUPPORTED_ARCH"]
            # black elements
            list_black = ["|", ";", "&", "<", ">", "`", "\\", "'", "\"",
                          "{", "}", "(", ")", "[", "]", "~", "*", "?",
                          "!", "\n"]
            for env in env_list:
                env = env.strip()
                if env == "":
                    continue
                if len(env.split()) != 2:
                    return
                if env.split()[0] == "umask" and env.split()[1] == "077":
                    continue
                for black in list_black:
                    flag = env.find(black)
                    if flag >= 0:
                        return
                if ((not env.startswith("export")) or (
                        env.split()[0] != "export")):
                    return
                else:
                    val = env[6:].strip()
                    if not val.find("="):
                        return
                    elif (val.split("=")[0].strip() not in list_white):
                        return
            self.result.rst = ResultStatus.OK
            self.result.val = "Mpprc file is ok"
        except Exception as e:
            self.result.rst = ResultStatus.NG
            self.result.val = "Can not read mpprc file"
