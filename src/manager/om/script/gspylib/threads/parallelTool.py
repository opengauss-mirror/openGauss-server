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
# Description  : ThreadPool.py is utility to support parallel control by
#                multiprocess
#############################################################################
import threading
import multiprocessing
import subprocess
import sys

from multiprocessing.dummy import Pool as ThreadPool


class parallelTool:
    '''
    Class for multi-parallel controling one-hosts
    '''
    DEFAULT_PARALLEL_NUM = 12

    def __init__(self):
        '''
        Constructor
        '''

    @staticmethod
    def getCpuCount(parallelJobs=0):
        """
        function: get cpu set of current board
                  cat /proc/cpuinfo |grep processor
        input: parallelJobs
        output: cpuSet
        """
        if (parallelJobs != 0):
            return parallelJobs
        # do this function to get the parallel number
        cpuSet = multiprocessing.cpu_count()
        if (cpuSet > 1):
            return cpuSet
        else:
            return parallelTool.DEFAULT_PARALLEL_NUM

    @staticmethod
    def parallelExecute(func, paraList, parallelJobs=0):
        """
        function: Execution of python functions through multiple processes
        input: func, list, parallelJobs
        output: list
        """
        jobs = parallelTool.getCpuCount(parallelJobs)
        if (jobs > len(paraList)):
            jobs = len(paraList)
        pool = ThreadPool(jobs)
        results = pool.map(func, paraList)
        pool.close()
        pool.join()
        return results


class CommandThread(threading.Thread):
    """
    The class is used to execute command in thread
    """

    def __init__(self, cmd):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.command = cmd
        self.cmdStauts = 0
        self.cmdOutput = ""

    def run(self):
        """
        function: Run command
        input : NA
        output: NA
        """
        (self.cmdStauts, self.cmdOutput) = subprocess.getstatusoutput(
            self.command)
