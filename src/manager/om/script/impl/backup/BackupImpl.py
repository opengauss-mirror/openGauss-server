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
# ----------------------------------------------------------------------------
# Description  : gs_backup is a utility to back up
# or restore binary files and parameter files.
#############################################################################
import sys


class BackupImpl:
    '''
    classdocs
    input : NA
    output: NA
    '''
    ACTION_BACKUP = "backup"
    ACTION_RESTORE = "restore"

    def __init__(self, backupObj):
        '''
        function: Constructor
        input : backupObj
        output: NA
        '''
        self.context = backupObj

    def run(self):
        '''
        function: main flow
        input : NA
        output: NA
        '''
        try:
            self.context.initLogger(self.context.action)
        except Exception as e:
            self.context.logger.closeLog()
            raise Exception(str(e))

        try:
            self.parseConfigFile()
            if self.context.action == BackupImpl.ACTION_BACKUP:
                self.doRemoteBackup()
            else:
                self.doRemoteRestore()
            self.context.logger.closeLog()
        except Exception as e:
            self.context.logger.logExit(str(e))

    def parseConfigFile(self):
        """
        function: Parsing configuration files
        input : NA
        output: NA
        """
        pass

    def doRemoteBackup(self):
        """
        function: Backup cluster config files
        input : NA
        output: NA
        """
        pass

    def doRemoteRestore(self):
        """
        function: Restore cluster config files
        input : NA
        output: NA
        """
        pass
