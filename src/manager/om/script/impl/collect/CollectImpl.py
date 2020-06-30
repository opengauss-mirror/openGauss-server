# coding: UTF-8
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
# Description  : gs_collector is a utility to collect information
# about the cluster.
#############################################################################
import sys


class CollectImpl:
    '''
    classdocs
    input : NA
    output: NA
    '''

    def __init__(self, collectObj):
        '''
        function: Constructor
        input : collectObj
        output: NA
        '''
        self.context = collectObj

    def run(self):
        '''
        function: main flow
        input : NA
        output: NA
        '''
        try:
            self.context.initLogger("gs_collector")
        except Exception as e:
            self.context.logger.closeLog()
            raise Exception(str(e))

        try:
            # Perform a log collection
            self.doCollector()

            self.context.logger.closeLog()
        except Exception as e:
            self.context.logger.logExit(str(e))

    def doCollector(self):
        """
        function: collect information
        input : strftime
        output: Successfully collected catalog statistics
        """
        pass
