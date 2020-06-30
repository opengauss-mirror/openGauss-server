#!/usr/bin/env python3
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
# Description  : GaussStat.py is utility for statistics
#############################################################################
import subprocess
import os
import sys
import socket
import glob
import pwd
import datetime
from random import sample
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(sys.path[0] + "/../../")
from gspylib.os.gsfile import g_file
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.threads.SshTool import SshTool
import gspylib.common.Sql as Sql

########################################################################
# Global variables define
########################################################################
INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2
# Limit multithreading to a maximum of 4
DEFAULT_PARALLEL_NUM = 4

ACTION_INSTALL_PMK = "install_pmk"
ACTION_COLLECT_STAT = "collect_stat"
ACTION_DISPLAY_STAT = "display_stat"
ACTION_ASYN_COLLECT = "asyn_collect"
ACTION_COLLECT_SINGLE_DN_INFO = "single_dn_info"

SQL_FILE_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                              "../etc/sql"))

g_recordList = {}
g_sessionCpuList = []
g_sessionMemList = []
g_sessionIOList = []
g_clusterInfo = None
g_DWS_mode = False


def isNumber(num):
    '''
    function: Judge if the variable is a number
    input : num
    output: bool
    '''
    try:
        ### try to convert num to float. if catch error, it means num
        # is not a number
        float(num)
    except Exception as e:
        return False
    return True


def isIp(ip):
    '''
    function: Judge if the variable is an ip address
    input : ip
    output: bool
    '''
    try:
        ### only support ipv4...
        socket.inet_aton(ip)
    except ImportError as e:
        return False
    return True


class statItem():
    '''
    Class for stating item
    '''

    def __init__(self, item_value, unit=None):
        '''
        function: initialize the parameters
        input : item_value, unit
        output: NA 
        '''
        # remove space
        item_value = item_value.strip()
        # judge if item is number
        if (isNumber(item_value)):
            self.value = item_value
        else:
            self.value = None
        self.unit = unit

    def __str__(self):
        '''
        function: create a string
        input : NA
        output: value 
        '''
        if not self.value:
            return ""
        elif self.unit:
            return "%-10s %s" % (self.value, self.unit)
        else:
            return "%s" % self.value


class clusterStatistics():
    '''
    Class for stating cluster message 
    '''

    def __init__(self):
        '''
        function: Constructor
        input : NA
        output: NA
        '''
        self.cluster_stat_generate_time = None

        ### Host cpu time
        self.cluster_host_total_cpu_time = None
        self.cluster_host_cpu_busy_time = None
        self.cluster_host_cpu_iowait_time = None
        self.cluster_host_cpu_busy_time_perc = None
        self.cluster_host_cpu_iowait_time_perc = None

        ### MPP cpu time
        self.cluster_mppdb_cpu_time_in_busy_time = None
        self.cluster_mppdb_cpu_time_in_total_time = None

        ### Shared buffer
        self.cluster_share_buffer_read = None
        self.cluster_share_buffer_hit = None
        self.cluster_share_buffer_hit_ratio = None

        ### In memory sort ratio
        self.cluster_in_memory_sort_count = None
        self.cluster_disk_sort_count = None
        self.cluster_in_memory_sort_ratio = None

        ### IO statistics
        self.cluster_io_stat_number_of_files = None
        self.cluster_io_stat_physical_reads = None
        self.cluster_io_stat_physical_writes = None
        self.cluster_io_stat_read_time = None
        self.cluster_io_stat_write_time = None

        ### Disk usage
        self.cluster_disk_usage_db_size = None
        self.cluster_disk_usage_tot_physical_writes = None
        self.cluster_disk_usage_avg_physical_write = None
        self.cluster_disk_usage_max_physical_write = None

        ### Activity statistics
        self.cluster_activity_active_sql_count = None
        self.cluster_activity_session_count = None


class nodeStatistics():
    '''
    Class for stating node message
    '''

    def __init__(self, nodename):
        '''
        function: Constructor
        input : nodename
        output: NA
        '''
        self.nodename = nodename
        self.node_mppdb_cpu_busy_time = None
        self.node_host_cpu_busy_time = None
        self.node_host_cpu_total_time = None
        self.node_mppdb_cpu_time_in_busy_time = None
        self.node_mppdb_cpu_time_in_total_time = None
        self.node_physical_memory = None
        self.node_db_memory_usage = None
        self.node_shared_buffer_size = None
        self.node_shared_buffer_hit_ratio = None
        self.node_in_memory_sorts = None
        self.node_in_disk_sorts = None
        self.node_in_memory_sort_ratio = None
        self.node_number_of_files = None
        self.node_physical_reads = None
        self.node_physical_writes = None
        self.node_read_time = None
        self.node_write_time = None


class sessionStatistics():
    '''
    Class for stating session message
    '''

    def __init__(self, nodename, dbname, username):
        '''
        function: Constructor
        input : nodename, dbname, username
        output: NA
        '''
        self.nodename = nodename
        self.dbname = dbname
        self.username = username
        self.session_cpu_time = None
        self.session_db_cpu_time = None
        self.session_cpu_percent = None

        self.session_buffer_reads = None
        self.session_buffer_hit_ratio = None
        self.session_in_memory_sorts = None
        self.session_in_disk_sorts = None
        self.session_in_memory_sorts_ratio = None
        self.session_total_memory_size = None
        self.session_used_memory_size = None

        self.session_physical_reads = None
        self.session_read_time = None


class GaussStat():
    '''
    Class for stating Gauss message
    '''

    def __init__(self, install_path="", user_name="", local_port="",
                 curr_time="", last_time="", snapshot_id="",
                 flag_num=0, master_host="", logger_fp=None, show_detail=False,
                 database_name="postgres"):
        '''
        function: Constructor
        input : install_path, user_name, local_port, curr_time, last_time,
                snapshot_id, flag_num, master_host, logger_fp, show_detail,
                database_name
        output: NA
        '''
        ### gsql paramter, must be set
        if not install_path or not user_name or not local_port or not \
                logger_fp:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50001"] %
                            "p or -c or -u or -d " + ".")
        else:
            self.installPath = install_path
            self.user = user_name
            self.localport = local_port
            self.logger = logger_fp

        if (curr_time == ""):
            self.currTime = "NULL"
        else:
            self.currTime = "'%s'" % curr_time

        if (last_time == ""):
            self.lastTime = "NULL"
        else:
            self.lastTime = "'%s'" % last_time

        if (snapshot_id == ""):
            self.snapshotId = "NULL"
        else:
            self.snapshotId = snapshot_id

        self.flagNum = flag_num
        self.masterHost = master_host

        ### show detail or not
        self.showDetail = show_detail

        ### which database we should connect.
        self.database = database_name

        ###initialize statistics 
        self.cluster_stat = clusterStatistics()
        self.node_stat = []
        self.session_cpu_stat = []
        self.session_mem_stat = []
        self.session_io_stat = []

        # internal parameter
        self.__baselineFlag = "gauss_stat_output_time"
        # default baseline check flag.
        self.__TopNSessions = 10

    def writeOutput(self, outstr):
        '''
        function: write output message
        input : outstr
        output: NA
        '''
        sys.stderr.write(outstr + "\n")
        sys.stderr.flush()

    def loadSingleNodeSessionCpuStat(self, connInfo):
        '''
        function: load single node(cn or dn) session cpu stat
        input : connInfo
        output: NA
        '''
        self.logger.debug("Loading single node session cpu stat on "
                          "the node [%s]." % connInfo[0])
        global g_sessionCpuList
        try:
            nodeName = connInfo[0]
            nodePort = connInfo[1]
            # when I query from pgxc_node, if I query from a cn node,
            # it will return all the logical nodes of the cluster.
            # this node is DN
            querySql = "SELECT node_name FROM DBE_PERF.node_name;"

            if (g_DWS_mode):
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                if (len(result) == 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = result[0][0]

                querySql = "SELECT o_node_name,o_db_name,o_user_name," \
                           "o_session_cpu_time,o_mppdb_cpu_time," \
                           "o_mppdb_cpu_time_perc \
                            FROM pmk.get_session_cpu_stat('%s', 10)" % \
                           pgxcNodeName

                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Load single node session cpu stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % result)
                if (len(result) != 0):
                    lines = []
                    for i in iter(result):
                        line = "|".join(i)
                        lines.append(line)
                    g_sessionCpuList.extend(lines)
            else:
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user, '',
                                                                 nodePort,
                                                                 "postgres")
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)

                if (output == ""):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = output.strip()

                querySql = "SELECT o_node_name,o_db_name,o_user_name," \
                           "o_session_cpu_time,o_mppdb_cpu_time," \
                           "o_mppdb_cpu_time_perc \
                            FROM pmk.get_session_cpu_stat('%s', 10)" % \
                           pgxcNodeName

                (status, output) = ClusterCommand.execSQLCommand(
                    querySql, self.user, '', nodePort, "postgres")
                self.logger.debug("Load single node session cpu stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)
                if (output != ""):
                    lines = output.split("\n")
                    g_sessionCpuList.extend(lines)
        except Exception as e:
            raise Exception(str(e))
        self.logger.debug("Successfully loaded single node session cpu stat "
                          "on the node [%s]." % connInfo[0])

    def loadSingleNodeSessionMemoryStat(self, connInfo):
        '''
        function: load single node(cn or dn) session memory stat
        input : connInfo
        output: NA
        '''
        self.logger.debug("Loading single node session memory stat on the "
                          "node [%s]." % connInfo[0])
        global g_sessionMemList
        try:
            nodeName = connInfo[0]
            nodePort = connInfo[1]
            # when I query from pgxc_node, if I query from a cn node,
            # it will return all the logical nodes of the cluster.
            # this node is DN
            querySql = "SELECT node_name FROM DBE_PERF.node_name;"

            if (g_DWS_mode):
                (status, result,
                 err_output) = ClusterCommand.excuteSqlOnLocalhost(nodePort,
                                                                   querySql)
                self.logger.debug(
                    "Get pgxc_node info "
                    "from the cluster. \ncommand: %s \nresult: %s." % (
                        querySql, result))

            if (g_DWS_mode):
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                if (len(result) == 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = result[0][0]

                querySql = "SELECT o_node_name ,o_db_name,o_user_name," \
                           "o_session_total_memory_size," \
                           "o_session_used_memory_size,\
                            o_buffer_hits,o_session_buffer_hit_ratio," \
                           "o_sorts_in_memory,o_sorts_in_disk," \
                           "o_session_memory_sort_ratio \
                            FROM pmk.get_session_memory_stat('%s', 10)" % \
                           pgxcNodeName

                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Load single node session memory stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)
                if (len(result) != 0):
                    lines = []
                    for i in iter(result):
                        line = "|".join(i)
                        lines.append(line)
                    g_sessionMemList.extend(lines)
            else:
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user, '',
                                                                 nodePort,
                                                                 "postgres")
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)

                if (output == ""):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = output.strip()

                querySql = "SELECT o_node_name ,o_db_name,o_user_name," \
                           "o_session_total_memory_size," \
                           "o_session_used_memory_size,\
                            o_buffer_hits,o_session_buffer_hit_ratio," \
                           "o_sorts_in_memory,o_sorts_in_disk," \
                           "o_session_memory_sort_ratio \
                            FROM pmk.get_session_memory_stat('%s', 10)" % \
                           pgxcNodeName

                (status, output) = ClusterCommand.execSQLCommand(
                    querySql, self.user, '', nodePort, "postgres")
                self.logger.debug("Load single node session memory stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)
                if (output != ""):
                    lines = output.split("\n")
                    g_sessionMemList.extend(lines)
        except Exception as e:
            raise Exception(str(e))
        self.logger.debug("Successfully loaded single node session "
                          "memory stat on the node [%s]." % connInfo[0])

    def loadSingleNodeSessionIOStat(self, connInfo):
        '''
        function: load single node(cn or dn) session IO stat
        input : connInfo
        output: NA
        '''
        self.logger.debug("Loading single node session IO stat "
                          "on the node [%s]." % connInfo[0])
        global g_sessionIOList
        try:
            nodeName = connInfo[0]
            nodePort = connInfo[1]
            # when I query from pgxc_node, if I query from a cn node,
            # it will return all the logical nodes of the cluster.
            # this node is DN
            querySql = "SELECT node_name FROM DBE_PERF.node_name;"
            if g_DWS_mode:
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if status != 2:
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                if len(result) == 0:
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = result[0][0]

                querySql = "SELECT o_node_name, o_db_name, " \
                           "o_user_name, o_disk_reads, o_read_time " \
                           "FROM pmk.get_session_io_stat('%s', 10)" \
                           % pgxcNodeName

                (status, result,
                 err_output) = ClusterCommand.excuteSqlOnLocalhost(nodePort,
                                                                   querySql)
                self.logger.debug(
                    "Load single node session io stat."
                    " \ncommand: %s \nresult: %s." % (
                        querySql, result))

                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Load single node session io stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   result))

                if status != 2:
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)
                if len(result) != 0:
                    lines = []
                    for i in iter(result):
                        line = "|".join(i)
                        lines.append(line)
                    g_sessionIOList.extend(lines)
            else:
                (status, output) = ClusterCommand.execSQLCommand(
                    querySql, self.user, '', nodePort, "postgres")
                self.logger.debug("Get pgxc_node info from the cluster. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if status != 0:
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)

                if output == "":
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                pgxcNodeName = output.strip()

                querySql = "SELECT o_node_name, o_db_name, " \
                           "o_user_name, o_disk_reads, o_read_time " \
                           "FROM pmk.get_session_io_stat('%s', 10)" \
                           % pgxcNodeName

                (status, output) = ClusterCommand.execSQLCommand(
                    querySql, self.user, '', nodePort, "postgres")
                self.logger.debug("Load single node session io stat. "
                                  "\ncommand: %s \nresult: %s." % (querySql,
                                                                   output))

                if status != 0:
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)
                if output != "":
                    lines = output.split("\n")
                    g_sessionIOList.extend(lines)
        except Exception as e:
            raise Exception(str(e))
        self.logger.debug("Successfully loaded single node session IO"
                          " stat on the node [%s]." % connInfo[0])

    def loadSingleNodeStat(self, connInfo):
        '''
        function: load single node(cn or dn) stat
        input : NA
        output: NA
        '''
        self.logger.debug("Loading single node stat on the node [%s]." %
                          connInfo[0])
        global g_recordList
        try:
            nodeName = connInfo[0]
            nodePort = connInfo[1]
            # when I query from pgxc_node, if I query from a cn node,
            # it will return all the logical nodes of the cluster.
            # this node is DN
            querySql = "SELECT node_name FROM DBE_PERF.node_name;"
            if (g_DWS_mode):
                (status, result,
                 err_output) = ClusterCommand.excuteSqlOnLocalhost(nodePort,
                                                                   querySql)
                self.logger.debug(
                    "Get pgxc_node info from cluster."
                    " \ncommand: %s \nresult: %s." % (
                        querySql, result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                if (len(result) == 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                recordList = result[0]
                if (len(recordList) != 1):
                    raise Exception(ErrorCode.GAUSS_517[
                                        "GAUSS_51700"]
                                    + "The record in recordList is:%s."
                                    % recordList)
                if (recordList[0] != ''):
                    recordList[0] = (recordList[0]).strip()
                    nodeType = 'D'

                # when I query from pgxc_node on a DB node, the node type is
                # 'C'. it's wrong, so I modify here.
                # when it is DB node, I specify the node type as 'D' selfly.
                # load single node stat
                if (dwsFlag):
                    skipSupperRoles = 'TRUE'
                else:
                    skipSupperRoles = 'FALSE'

                instType = 'D'

                querySql = "SELECT * " \
                           "FROM pmk.load_node_stat(%s, %s, %s, '%s'" \
                           ", '%s', %s)" % \
                           (self.currTime, self.lastTime, self.snapshotId,
                            recordList[0], instType, skipSupperRoles)
                (status, result,
                 err_output) = ClusterCommand.excuteSqlOnLocalhost(nodePort,
                                                                   querySql)
                self.logger.debug(
                    "Load single node stat. \ncommand: %s \nresult: %s." % (
                        querySql, result))

                querySql = "SELECT * FROM pmk.load_node_stat(%s, %s, %s, " \
                           "'%s', '%s', %s)" % \
                           (self.currTime, self.lastTime, self.snapshotId,
                            recordList[0], instType, skipSupperRoles)
                (status, result,
                 err_output) = ClusterCommand.excuteSqlOnLocalhost(nodePort,
                                                                   querySql)
                self.logger.debug(
                    "Load single node stat. \ncommand: %s \nresult: %s." % (
                        querySql, result))

                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                if (len(result) == 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                output = ""
                if (len(result) != 0):
                    for i in iter(result):
                        line = "|".join(i)
                        output += line + "\n"
                g_recordList[recordList[0]] = output
            else:
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user, '',
                                                                 nodePort,
                                                                 "postgres")
                self.logger.debug(
                    "Get node info from cluster."
                    " \ncommand: %s \nresult: %s." % (
                        querySql, output))
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513[
                                        "GAUSS_51300"] % querySql
                                    + " Error:\n%s" % output)
                if (output == ""):
                    raise Exception(ErrorCode.GAUSS_513[
                                        "GAUSS_51300"] % querySql
                                    + " Return record is null")
                recordList = output.split('|')
                if (len(recordList) != 1):
                    raise Exception(ErrorCode.GAUSS_517[
                                        "GAUSS_51700"]
                                    + "The record in recordList is:%s."
                                    % recordList)
                if (recordList[0] != ''):
                    recordList[0] = (recordList[0]).strip()
                    nodeType = 'D'

                # when I query from pgxc_node on a DB node, the node type is
                # 'C'. it's wrong, so I modify here.
                # when it is DB node, I specify the node type as 'D' selfly.
                # load single node stat
                if (dwsFlag):
                    skipSupperRoles = 'TRUE'
                else:
                    skipSupperRoles = 'FALSE'

                instType = 'D'

                querySql = \
                    "SELECT * FROM pmk.load_node_stat(%s, " \
                    "%s, %s, '%s', '%s', %s)" % \
                    (self.currTime, self.lastTime, self.snapshotId,
                     recordList[0], instType, skipSupperRoles)
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user, '',
                                                                 nodePort,
                                                                 "postgres")
                self.logger.debug(
                    "Load single node stat. \ncommand: %s \nresult: %s." % (
                        querySql, output))

                querySql = "SELECT * FROM pmk.load_node_stat(%s, %s, %s, " \
                           "'%s', '%s', %s)" % \
                           (self.currTime, self.lastTime, self.snapshotId,
                            recordList[0], instType, skipSupperRoles)
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user, '',
                                                                 nodePort,
                                                                 "postgres")
                self.logger.debug(
                    "Load single node stat. \ncommand: %s \nresult: %s." % (
                        querySql, output))

                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)

                if (output == ""):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Return record is null")

                g_recordList[recordList[0]] = output
        except Exception as e:
            raise Exception(str(e))
        self.logger.debug("Successfully loaded single node stat on the "
                          "node [%s]." % connInfo[0])

    def checkRoleOfDnInst(self, dnDataDir):
        '''
        function: check role of DB Inst
        input : dnDataDir
        output: NA
        '''
        self.logger.debug("Checking role of database node instance.")
        try:
            if (not os.path.exists(os.path.join(dnDataDir, "postmaster.pid"))):
                return False

            checkCmd = "gs_ctl query -D %s | grep 'HA state' -A 1 | grep " \
                       "'local_role'" % dnDataDir
            (status, output) = DefaultValue.retryGetstatusoutput(checkCmd)
            if (status != 0):
                cmd = "gs_ctl query -D %s" % dnDataDir
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0 and output.find("could not connect to "
                                                "the local server") > 0):
                    return False
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                checkCmd + " Error:\n%s." % output
                                + "The cmd is %s" % cmd)

            roleStatus = ((output.split(':'))[1]).strip()
            if (roleStatus == "Primary"):
                return True
            else:
                return False
        except  Exception as e:
            raise Exception(str(e))

    def getPGXCNode(self):
        '''
        function: get pgxc node of the cluster,contains CN and master DNs
        input : NA
        output: pgxcNodeList
        '''
        self.logger.debug("Getting pgxc node of the cluster.")
        pgxcNodeList = []
        nodeItem = []
        nodeName = ""
        nodePort = 0
        try:
            # get node info
            nodeInfo = g_clusterInfo.getDbNodeByName(
                DefaultValue.GetHostIpOrName())
            for dnInst in nodeInfo.datanodes:
                if (dnInst.instanceType != DUMMY_STANDBY_INSTANCE):
                    if self.checkRoleOfDnInst(dnInst.datadir) or len(
                            nodeInfo.datanodes) == 1:
                        nodeName = ""
                        nodePort = "%s" % dnInst.port
                        nodeItem = []
                        nodeItem.append(nodeName)
                        nodeItem.append(nodePort)
                        pgxcNodeList.append(nodeItem)
            return pgxcNodeList
        except Exception as e:
            raise Exception(str(e))

    def getPerfCheckPsqlCommand(self, dbname):
        """
        """
        cmd = ClusterCommand.getSQLCommand(self.localport, dbname,
                                           os.path.join(self.installPath,
                                                        "bin/gsql"))
        return cmd

    ### NOTICE: itemCounts must be more than two. so that we can distinguish
    # records and (%d rows)
    def execQueryCommand(self, sql, itemCounts, collectNum, baselineflag=""):
        '''
        function: execute the query command
        input : sql, itemCounts, collectNum, baselineflag
        output: NA 
        '''
        if (baselineflag == ""):
            baselineflag = self.__baselineFlag

        # save sql statement to file to reduce quot nesting
        sqlFile = os.path.join(DefaultValue.getTmpDirFromEnv(),
                               "checkperf_query_%s_%s.sql" % (os.getpid(),
                                                              collectNum))
        if (dwsFlag):
            sql = "set cgroup_name='Rush';" + sql

        cmd = "echo \"%s\" > '%s' && chown %s '%s'" % (sql, sqlFile, self.user,
                                                       sqlFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            "SQL statement to file" + "\nCommand:\n  "
                                                      "%s\nError:\n  %s" % (
                                cmd, output))

        try:
            sql_cmd = self.getPerfCheckPsqlCommand(self.database)
            if (os.getuid() == 0):
                cmd = "su - %s -c \'%s -f %s -X " \
                      "--variable=ON_ERROR_STOP=on\' " \
                      "2>/dev/null" % (self.user, sql_cmd, sqlFile)
            else:
                cmd = "%s -f %s -X --variable=ON_ERROR_STOP=on 2>/dev/null" % \
                      (sql_cmd, sqlFile)
            self.logger.debug("Execute command: %s" % (cmd))

            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 or ClusterCommand.findErrorInSqlFile(sqlFile,
                                                                output):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % cmd +
                                " Error: \n%s" % output)

            DefaultValue.cleanTmpFile(sqlFile)

            baseline = self.checkExpectedOutput(output, baselineflag, False)

            if (baseline == -1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                ("Cannot fetch query baseline. Error: \n%s" %
                                 (output)))

            if (self.checkExpectedOutput(output, "(0 rows)", True,
                                         baseline) != -1):
                ### can not support now
                return None

            lines = output.split("\n")
            linesCount = len(lines)

            ### result must more than 4 lines
            if (linesCount <= baseline + 4):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                "Unexpected lines" + "  Error: \n%s" % (
                                    output))

            records = []
            for ino in range(baseline + 2, linesCount):
                line = lines[ino]
                record = line.split("|")
                if (len(record) != itemCounts):
                    break
                records.append(record)

            self.logger.debug("Query command succeeded.")
            self.logger.debug("Query results: \n%s." % str(records))
            return records
        except Exception as e:
            ### execute query command failed. log and raise
            self.logger.debug("Failed to execute the command of query [%s] "
                              "on local host." % sql)
            DefaultValue.cleanTmpFile(sqlFile)
            raise Exception(str(e))

    ## check if the expected line existed in output.
    def checkExpectedOutput(self, output, expect, strict=True, starter=0):
        '''
        function: check expected output
        input : output, expect, strict, starter
        output: NA
        '''
        lines = output.split("\n")
        expect = expect.strip()
        if (starter < len(lines)):
            for i in range(starter, len(lines)):
                line = lines[i]
                if (strict):
                    if (expect == line.strip()):
                        return i - starter
                else:
                    if (line.strip().find(expect) != -1):
                        return i - starter
        return -1

    def CheckInstanceMode(self):
        '''
        function: test coordinator and datanode mode
        input : NA
        output: NA
        '''
        try:
            self.logger.debug("Checking the coordinator and datanode mode .")

            ### test coordinator and coordinator mode...
            cmd = "ps ux|awk '{if($11 == \"%s\")print $0}'" % os.path.join(
                self.installPath, "bin/gaussdb")
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 or output.strip() == ""):
                self.logger.debug("Failed to test the CN and CN's mode "
                                  "with the user name. Error: \n%s." % (output)
                                  + "The cmd is %s" % cmd)
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51605"] % "CN" +
                                " Please check the cluster status.")

            self.logger.debug("Test CN output:\n%s" % output)
            if (self.checkExpectedOutput(output, "--restoremode",
                                         False) != -1):
                self.logger.debug("CN is running in restore mode.")
                raise Exception(ErrorCode.GAUSS_512["GAUSS_51212"] %
                                "running CN instance in normal mode")
            elif (self.checkExpectedOutput(output, "--coordinator",
                                           False) != -1):
                self.logger.debug("CN is running in normal mode.")
            else:
                self.logger.debug("There is no running CN instance "
                                  "on this node.")
                raise Exception(ErrorCode.GAUSS_512["GAUSS_51212"] %
                                "running CN instance on this node")

            self.logger.debug("Successfully checked the coordinator and "
                              "datanode mode .")
        except Exception as e:
            ### execute query command failed. log and raise
            self.logger.debug("Failed to check coordinator and datanode mode.")
            raise Exception(str(e))

    def installPMKInDWSMode(self):
        '''
        function: install PMK shcema in DWS mode
        input : NA
        output: NA
        '''
        try:
            ### test pmk schema exist or not.
            pmk_cmd = "select oid from pg_namespace where nspname='pmk'"
            class_cmd = "select count(1) from pg_class where " \
                        "relnamespace=(select oid from pg_namespace where " \
                        "nspname='pmk')"
            proc_cmd = "select count(1) from pg_proc where " \
                       "pronamespace=(select oid from pg_namespace where " \
                       "nspname='pmk')"
            (pmk_status, pmkResult, pmk_error) = \
                ClusterCommand.excuteSqlOnLocalhost(self.localport, pmk_cmd)
            (class_status, classResult, class_error) = \
                ClusterCommand.excuteSqlOnLocalhost(self.localport, class_cmd)
            (proc_status, procResult, proc_error) = \
                ClusterCommand.excuteSqlOnLocalhost(self.localport, proc_cmd)
            self.logger.debug("Test PMK schema. Output: \n%s %s %s." %
                              (pmkResult, classResult, procResult))

            tablespace = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
            if (pmk_status != 2):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53028"]
                                % pmk_error)
            if (class_status != 2):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53028"]
                                % class_error)
            if proc_status != 2:
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53028"]
                                % proc_error)

            if (len(pmkResult) == 0):
                ### schema not exist, so we create it.
                self.logger.debug("PMK schema does not exist. "
                                  "Install it for the first time.")
            elif ((str(classResult[0][0]).strip() == "13" and
                   str(procResult[0][0]).strip() == "29")):
                ### schema already created.
                self.logger.debug("PMK schema is already exist.")
                return
            elif ((str(classResult[0][0]).strip() == "5" and
                   str(procResult[0][0]).strip() == "29")):
                ### schema already created.
                self.logger.debug("PMK schema is already exist.")
                return
            else:
                ### maybe class count or proc count not the same.
                self.logger.debug("PMK schema is incomplete. Try to "
                                  "execute \"drop schema pmk cascade;\".")
                drop_cmd = "drop schema pmk cascade"
                (drop_status, drop_result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport,
                                                        drop_cmd)
                if err_output != "":
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    drop_cmd + " Error: \n%s" % err_output)
                else:
                    self.logger.debug("Successfully dropped schema PMK.")

            ### add pmk schema to database.
            err_output = ""

            if tablespace is not None and tablespace != "":
                for i in iter(Sql.PMK_NEW):
                    (status, result, err_output) = \
                        ClusterCommand.excuteSqlOnLocalhost(self.localport, i)
                    if err_output != "":
                        self.logger.debug("Failed to install pmk schema,"
                                          "Error: \n%s" % err_output)
                        break
            else:
                for i in iter(Sql.PMK_ORIGINAL):
                    (status, result, err_output) = \
                        ClusterCommand.excuteSqlOnLocalhost(self.localport, i)
                    if err_output != "":
                        self.logger.debug("Failed to install pmk schema,"
                                          "Error: \n%s" % err_output)
                        break

            # Determine the execution result of the pmk installation
            if err_output != "":
                dropSchemaCmd = "drop schema if exists pmk cascade"
                (status, result, err1_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport,
                                                        dropSchemaCmd)
                if err1_output != "":
                    self.logger.debug("Failed to drop schema PMK. "
                                      "Error: \n%s" % err1_output)
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53029"]
                                % err_output)
        except Exception as e:
            raise Exception(str(e))

    def installPMKInNonDWSMode(self):
        '''
        function: install PMK shcema in non DWS mode
        input : NA
        output: NA
        '''
        try:
            test_data_node_file = os.path.join(SQL_FILE_PATH,
                                               "test_data_node.sql")
            test_pmk_file = os.path.join(SQL_FILE_PATH, "test_pmk.sql")
            gsql_path = os.path.join(self.installPath, "bin/gsql")
            tablespace = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
            pmk_schema_num1 = "pmk schema exist. class count is 13, " \
                              "proc count is 29"
            pmk_schema_num2 = "pmk schema exist. class count is 5, " \
                              "proc count is 29"

            if (not g_clusterInfo.isSingleInstCluster()):
                if (os.getuid() == 0):
                    cmd = "su - %s -c \'%s -U %s -p %s -d %s -X " \
                          "--variable=ON_ERROR_STOP=on -f " % \
                          (self.user, gsql_path, self.user,
                           str(self.localport), self.database)
                    cmd += "%s" % test_data_node_file
                    cmd += "\'"
                else:
                    cmd = "%s -U %s -p %s -d %s -X " \
                          "--variable=ON_ERROR_STOP=on -f " % \
                          (gsql_path, self.user, str(self.localport),
                           self.database)
                    cmd += "%s" % test_data_node_file

                (status, output) = subprocess.getstatusoutput(cmd)
                self.logger.debug("Command for testing node: %s" % cmd)
                self.logger.debug("Output for testing node: %s" % output)
                if (status != 0 or ClusterCommand.findErrorInSqlFile(
                        test_data_node_file, output)):
                    self.logger.debug(
                        "Failed to query dataNode. Error: \n%s" % output)
                    raise Exception(ErrorCode.GAUSS_513[
                                        "GAUSS_51300"] % cmd
                                    + " Error: \n%s" % output)

                lines = output.split("\n")
                if (len(lines) < 4 or self.checkExpectedOutput(
                        output, "(0 rows)") >= 2):
                    self.logger.debug("No database node is "
                                      "configured in cluster.")
                    raise Exception(ErrorCode.GAUSS_512["GAUSS_51212"] %
                                    "configured database node in cluster")

            ### test pmk schema exist or not.
            if (os.getuid() == 0):
                cmd = "su - %s -c \'%s -U %s -p %s -d %s -X -f " % \
                      (self.user, gsql_path, self.user, str(self.localport),
                       self.database)
                cmd += "%s" % (test_pmk_file)
                cmd += "\'"
            else:
                cmd = "%s -U %s -p %s -d %s -X -f " % \
                      (gsql_path, self.user, str(self.localport),
                       self.database)
                cmd += "%s" % (test_pmk_file)

            (status, output) = subprocess.getstatusoutput(cmd)
            self.logger.debug("Test PMK schema. Output: \n%s." % (output))
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % cmd +
                                " Error: \n%s" % output)

            if (self.checkExpectedOutput(output, "ERROR:  query returned no "
                                                 "rows", False) != -1):
                ### schema not exist, so we create it.
                self.logger.debug("PMK schema does not exist. Install it for"
                                  " the first time.")

            elif (self.checkExpectedOutput(output, pmk_schema_num1, False) !=
                  -1 and not tablespace):
                ### schema already created.
                self.logger.debug("PMK schema is already exist.")
                return
            elif (self.checkExpectedOutput(output, pmk_schema_num2, False) !=
                  -1 and tablespace):
                ### schema already created.
                self.logger.debug("PMK schema is already exist.")
                return
            else:
                ### maybe class count or proc count not the same.
                self.logger.debug("PMK schema is incomplete. Try to "
                                  "execute \"drop schema pmk cascade;\".")
                if (os.getuid() == 0):
                    cmd = "su - %s -c 'gsql -d %s -p %s -X -c \"drop " \
                          "schema pmk cascade;\"'" % \
                          (self.user, self.database, str(self.localport))
                else:
                    cmd = "gsql -d %s -p %s -X -c \"drop schema " \
                          "pmk cascade;\"" % \
                          (self.database, str(self.localport))
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0 or ClusterCommand.findErrorInSql(
                        output) == True):
                    self.logger.debug("Failed to drop schema PMK. "
                                      "Error: \n%s" % output)
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    cmd + " Error: \n%s" % output)
                else:
                    self.logger.debug("Successfully dropped schema PMK: %s." %
                                      output)

            if (g_clusterInfo.isSingleInstCluster()):
                pmkSqlFile = (os.path.join(
                    self.installPath,
                    "share/postgresql/pmk_schema_single_inst.sql"))
            else:
                pmkSqlFile = (os.path.join(
                    self.installPath, "share/postgresql/pmk_schema.sql"))
            pmkSqlFile_back = (os.path.join(
                self.installPath, "share/postgresql/pmk_schema_bak.sql"))
            cmd = "cp '%s' '%s'" % (pmkSqlFile, pmkSqlFile_back)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s" % output)

            if (tablespace is not None and tablespace != ""):
                cmd = "sed -i \"s/START TRANSACTION;//g\" %s && " % \
                      pmkSqlFile_back
                cmd += "sed -i \"s/COMMIT;//g\" %s && " % pmkSqlFile_back
                cmd += "sed -i \"/PRIMARY KEY/d\" %s && " % pmkSqlFile_back
                cmd += "sed -i \"/CREATE INDEX/d\" %s && " % pmkSqlFile_back
                cmd += "sed -i \"1i\\SET default_tablespace = %s;\" %s" % \
                       (tablespace, pmkSqlFile_back)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    DefaultValue.cleanTmpFile(pmkSqlFile_back)
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                    " Error: \n%s" % output)
            else:
                if (not g_clusterInfo.isSingleInstCluster()):
                    self.logger.debug("Set installation groupt to "
                                      "default_storage_nodegroup in this "
                                      "session.")
                    sql_nodegroup = "SELECT group_name FROM " \
                                    "pg_catalog.pgxc_group WHERE " \
                                    "is_installation='t';"
                    if (os.getuid() == 0):
                        cmd = "su - %s -c 'gsql -d %s -p %s -x -A -c " \
                              "\"%s\"'" % \
                              (self.user, self.database, str(self.localport),
                               sql_nodegroup)
                    else:
                        cmd = "gsql -d %s -p %s -x -A -c \"%s\"" % \
                              (self.database, str(self.localport),
                               sql_nodegroup)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0 or ClusterCommand.findErrorInSql(output)):
                        self.logger.debug("Failed to get installation groupt. "
                                          "Error: \n%s" % output)
                        raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                        cmd + " Error: \n%s" % output)

                    installation_groupt = output.split('|')[-1]
                    cmd = "sed -i \"1i\\SET default_storage_nodegroup = " \
                          "%s;\" %s" % \
                          (installation_groupt, pmkSqlFile_back)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        DefaultValue.cleanTmpFile(pmkSqlFile_back)
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                        cmd + " Error: \n%s" % output)
                    self.logger.debug("Successfully set "
                                      "default_storage_nodegroup is %s in "
                                      "this session." % \
                                      installation_groupt)

            ### add pmk schema to database.
            if (os.getuid() == 0):
                cmd = "su - %s -c \'%s -U %s -p %s -d %s -X " \
                      "--variable=ON_ERROR_STOP=on -f \"" % \
                      (self.user, gsql_path, self.user, str(self.localport),
                       self.database)
                cmd += "%s" % pmkSqlFile_back
                cmd += "\"\'"
            else:
                cmd = "%s -U %s -p %s -d %s -X " \
                      "--variable=ON_ERROR_STOP=on -f \"" % \
                      (gsql_path, self.user, str(self.localport),
                       self.database)
                cmd += "%s" % pmkSqlFile_back
                cmd += "\""

            (status, output) = subprocess.getstatusoutput(cmd)
            self.logger.debug("Create pmk output:%s" % output)
            # Determine the execution result of the pmk installation
            if (status != 0 or ClusterCommand.findErrorInSqlFile(
                    pmkSqlFile_back, output)):
                # Determine whether the current user is the root user
                if (os.getuid() == 0):
                    # Link under the root user command
                    dropSchemaCmd = "su - %s -c '%s -d %s -p %s -X -c " \
                                    "\"drop schema if exists pmk " \
                                    "cascade;\"' " % \
                                    (self.user, gsql_path, self.database,
                                     str(self.localport))
                else:
                    # Link under the cluster user command
                    dropSchemaCmd = "%s -d %s -p %s -X -c \"drop schema if " \
                                    "exists pmk cascade;\"" % \
                                    (gsql_path, self.database,
                                     str(self.localport))
                (status, output1) = subprocess.getstatusoutput(dropSchemaCmd)
                # Judge the results of the fallback installation pmk
                if (status != 0 or ClusterCommand.findErrorInSql(output1)):
                    self.logger.debug("Failed to drop schema PMK. Error: "
                                      "\n%s" % output1)
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % cmd +
                                " Error: \n%s" % output)
        except Exception as e:
            ### execute query command failed. log and raise
            self.logger.debug("Failed to check coordinator and datanode mode.")
            raise Exception(str(e))

    def installPMKSchema(self):
        '''
        function: install PMK shcema
        input : NA
        output: NA
        '''
        try:
            self.logger.debug("Installing PMK schema.")

            if (not g_clusterInfo.isSingleInstCluster()):
                # test DB mode
                self.CheckInstanceMode()

            if (g_DWS_mode):
                self.installPMKInDWSMode()
            else:
                self.installPMKInNonDWSMode()
            ### create schema success.
            self.logger.debug("Successfully installed PMK schema.")
        except Exception as e:
            raise Exception(str(e))

    def collectStat(self, act):
        '''
        function: collect performance statistics
        input : act
        output: NA
        '''
        try:
            self.logger.debug("Collecting each performance statistics [%s]." %
                              act)
            if (act == "ClusterHostCpuStat"):
                # collect cluster host CPU statistics
                self.collectClusterHostCpuStat()
            elif (act == "ClusterMPPDBCpuStat"):
                # collect MPPDB CPU statistics
                self.collectClusterMPPDBCpuStat()
            elif (act == "ShareBufferStat"):
                # collect share buffer statistics
                self.collectShareBufferStat()
            elif (act == "ClusterSortStat"):
                # collect sort statistics
                self.collectClusterSortStat()
            elif (act == "ClusterIOStat"):
                # collect IO statistics
                self.collectClusterIOStat()
            elif (act == "ClusterDiskStat"):
                # collect disk usage statistics
                self.collectClusterDiskStat()
            elif (act == "ClusterActiveSqlCount"):
                # collect active SQL statistics
                self.collectClusterActiveSqlCount()
            elif (act == "ClusterSessionCount"):
                # collect session count statistics
                self.collectClusterSessionCount()
            elif (act == "NodeCpuStat"):
                # collect node CPU statistics
                self.collectNodeCpuStat()
            elif (act == "NodeMemoryStat"):
                # collect node memory statistics
                self.collectNodeMemoryStat()
            elif (act == "NodeIOStat"):
                # collect node IO statistics
                self.collectNodeIOStat()
            elif (act == "SessionCpuStat"):
                # collect session CPU statistics
                self.collectSessionCpuStat()
            elif (act == "SessionMemoryStat"):
                # collect session memory statistics
                self.collectSessionMemoryStat()
            elif (act == "SessionIOStat"):
                # collect session IO statistics
                self.collectSessionIOStat()
            self.logger.debug("Successfully collected each performance "
                              "statistics [%s]." % act)
        except Exception as e:
            raise Exception(str(e))

    def collectPGXCNodeStat(self, pgxcNodeList):
        '''
        function: collect PGXC node performance statistics
        input : pgxcNodeList
        output: NA
        '''
        recordTempFile = ""
        try:
            self.logger.debug("Collecting PGXC node performance statistics.")
            if (len(pgxcNodeList) != 0):
                # load pgxc node statistics parallel
                pool = ThreadPool(DefaultValue.getCpuSet())
                pool.map(self.loadSingleNodeStat, pgxcNodeList)
                pool.close()
                pool.join()
                for record in g_recordList.keys():
                    self.logger.debug("%s:  %s\n" % (record,
                                                     g_recordList[record]))
            else:
                return

            # create a temp file for records write
            strCmd = ""
            recordTempFile = os.path.join(DefaultValue.getTmpDirFromEnv(),
                                          "recordTempFile_%d_%s" % \
                                          (self.flagNum,
                                           DefaultValue.GetHostIpOrName()))

            # clean the temp file first
            DefaultValue.cleanTmpFile(recordTempFile)
            # write records into the temp file
            for record in g_recordList.keys():
                strCmd += "%s::::%s\n" % (record, g_recordList[record])

            g_file.createFileInSafeMode(recordTempFile)
            with open(recordTempFile, 'w') as fp:
                fp.writelines(strCmd)

            if self.masterHost != DefaultValue.GetHostIpOrName():
                # scp record Temp File to tmpDir
                scpCmd = " pscp -H %s '%s' '%s/'" % (
                    self.masterHost, recordTempFile,
                    DefaultValue.getTmpDirFromEnv())

                self.logger.debug("Execute command: %s" % scpCmd)
                (status, output) = subprocess.getstatusoutput(scpCmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                                    "record temp file" + " Error: \n%s" %
                                    output)
            self.logger.debug("Successfully collected PGXC node "
                              "performance statistics.")
        except Exception as e:
            # close and remove temporary file
            DefaultValue.cleanTmpFile(recordTempFile)
            raise Exception(str(e))

    def collectSessionCpuStatNew(self, pgxcNodeList):
        '''
        function: collect session cpu performance statistics
        input : pgxcNodeList
        output: NA
        '''
        sessionCpuTempFile = ""
        try:
            self.logger.debug("Collecting session cpu performance statistics.")
            if (len(pgxcNodeList) != 0):
                # load session cpu statistics parallel
                pool = ThreadPool(DefaultValue.getCpuSet())
                results = pool.map(self.loadSingleNodeSessionCpuStat,
                                   pgxcNodeList)
                pool.close()
                pool.join()
                for record in g_sessionCpuList:
                    self.logger.debug("g_sessionCpuList:  %s\n" % record)
            else:
                return

            # create a temp file for records write
            strCmd = ""
            sessionCpuTempFile = os.path.join(DefaultValue.getTmpDirFromEnv(),
                                              "sessionCpuTempFile_%d_%s" % \
                                              (self.flagNum,
                                               DefaultValue.GetHostIpOrName()))

            # clean the temp file first
            DefaultValue.cleanTmpFile(sessionCpuTempFile)
            # write records into the temp file
            for record in g_sessionCpuList:
                strCmd += "%s\n" % record

            g_file.createFileInSafeMode(sessionCpuTempFile)
            with open(sessionCpuTempFile, 'w') as fp:
                fp.writelines(strCmd)

            if self.masterHost != DefaultValue.GetHostIpOrName():
                # scp session Cpu Temp File to tmpDir
                scpCmd = "pscp -H %s '%s' '%s'/" % (
                    self.masterHost, sessionCpuTempFile,
                    DefaultValue.getTmpDirFromEnv())

                self.logger.debug("Execute command: %s" % scpCmd)
                (status, output) = subprocess.getstatusoutput(scpCmd)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                                    "record temp file" + " Error: \n%s" %
                                    output)

                # close and remove temporary file
                DefaultValue.cleanTmpFile(sessionCpuTempFile)

            self.logger.debug("Successfully collected session "
                              "cpu performance statistics.")
        except Exception as e:
            # close and remove temporary file
            DefaultValue.cleanTmpFile(sessionCpuTempFile)
            raise Exception(str(e))

    def collectSessionMemStatNew(self, pgxcNodeList):
        '''
        function: collect session memory performance statistics
        input : pgxcNodeList
        output: NA
        '''
        sessionMemTempFile = ""
        try:
            self.logger.debug("Collecting session memory "
                              "performance statistics.")
            if (len(pgxcNodeList) != 0):
                # load session memory statistics parallel
                pool = ThreadPool(DefaultValue.getCpuSet())
                results = pool.map(self.loadSingleNodeSessionMemoryStat,
                                   pgxcNodeList)
                pool.close()
                pool.join()
                for record in g_sessionMemList:
                    self.logger.debug("g_sessionMemList:  %s\n" % record)
            else:
                return

                # create a temp file for records write
            strCmd = ""
            sessionMemTempFile = os.path.join(
                DefaultValue.getTmpDirFromEnv(),
                "sessionMemTempFile_%d_%s" % (
                    self.flagNum, DefaultValue.GetHostIpOrName()))

            # clean the temp file first
            DefaultValue.cleanTmpFile(sessionMemTempFile)
            # write records into the temp file
            for record in g_sessionMemList:
                strCmd += "%s\n" % record

            g_file.createFileInSafeMode(sessionMemTempFile)
            with open(sessionMemTempFile, 'w') as fp:
                fp.writelines(strCmd)

            if self.masterHost != DefaultValue.GetHostIpOrName():
                # scp session Mem TempFile to tmpDir
                scpCmd = "pscp -H %s '%s' '%s'/" % (
                    self.masterHost, sessionMemTempFile,
                    DefaultValue.getTmpDirFromEnv())

                self.logger.debug("Execute command: %s" % scpCmd)
                (status, output) = subprocess.getstatusoutput(scpCmd)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                                    "record temp file" + " Error: \n%s" %
                                    output)

                # close and remove temporary file
                DefaultValue.cleanTmpFile(sessionMemTempFile)

            self.logger.debug("Successfully collected session memory "
                              "performance statistics.")
        except Exception as e:
            # close and remove temporary file
            DefaultValue.cleanTmpFile(sessionMemTempFile)
            raise Exception(str(e))

    def collectSessionIOStatNew(self, pgxcNodeList):
        '''
        function: collect session io performance statistics
        input : pgxcNodeList
        output: NA
        '''
        sessionIOTempFile = ""
        try:
            self.logger.debug("Collecting session IO performance statistics.")
            if (len(pgxcNodeList) != 0):
                # load session IO statistics parallel
                pool = ThreadPool(DefaultValue.getCpuSet())
                results = pool.map(self.loadSingleNodeSessionIOStat,
                                   pgxcNodeList)
                pool.close()
                pool.join()
                for record in g_sessionIOList:
                    self.logger.debug("g_sessionIOList:  %s\n" % record)
            else:
                return

            # create a temp file for records write
            strCmd = ""
            sessionIOTempFile = os.path.join(DefaultValue.getTmpDirFromEnv(),
                                             "sessionIOTempFile_%d_%s" %
                                             (self.flagNum,
                                              DefaultValue.GetHostIpOrName()))

            # clean the temp file first
            DefaultValue.cleanTmpFile(sessionIOTempFile)
            # write records into the temp file
            for record in g_sessionIOList:
                strCmd += "%s\n" % record

            g_file.createFileInSafeMode(sessionIOTempFile)
            with open(sessionIOTempFile, 'w') as fp:
                fp.writelines(strCmd)

            if self.masterHost != DefaultValue.GetHostIpOrName():
                # scp session IO Temp File to tmpDir
                scpCmd = "pscp -H %s '%s' '%s'/" % (
                    self.masterHost, sessionIOTempFile,
                    DefaultValue.getTmpDirFromEnv())

                self.logger.debug("Execute command: %s" % scpCmd)
                (status, output) = subprocess.getstatusoutput(scpCmd)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                                    "record temp file" + " Error: \n%s" %
                                    output)

                # close and remove temporary file
                DefaultValue.cleanTmpFile(sessionIOTempFile)

            self.logger.debug("Successfully collected session IO "
                              "performance statistics.")
        except Exception as e:
            # close and remove temporary file
            DefaultValue.cleanTmpFile(sessionIOTempFile)
            raise Exception(str(e))

    def cleanTempFiles(self):
        """
        """
        # clean all the temp files before start collect the performance data
        recordTempFilePattern = os.path.join(DefaultValue.getTmpDirFromEnv(),
                                             'recordTempFile_*_*')
        recordTempFileList = glob.iglob(r'%s' % recordTempFilePattern)
        for tempFile in recordTempFileList:
            os.remove(tempFile)

        sessionCpuTempFilePattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionCpuTempFile_*_*')
        sessionCpuTempFileList = glob.iglob(r'%s' % sessionCpuTempFilePattern)
        for tempFile in sessionCpuTempFileList:
            os.remove(tempFile)

        sessionMemTempFilePattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionMemTempFile_*_*')
        sessionMemTempFileList = glob.iglob(r'%s' % sessionMemTempFilePattern)
        for tempFile in sessionMemTempFileList:
            os.remove(tempFile)

        sessionIOTempFilePattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionIOTempFile_*_*')
        sessionIOTempFileList = glob.iglob(r'%s' % sessionIOTempFilePattern)
        for tempFile in sessionIOTempFileList:
            os.remove(tempFile)

        sessionCpuTempResultPattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionCpuTempResult_*_*')
        sessionCpuTempResultList = glob.iglob(r'%s' %
                                              sessionCpuTempResultPattern)
        for tempFile in sessionCpuTempResultList:
            os.remove(tempFile)

        sessionMemTempResultPattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionMemTempResult_*_*')
        sessionMemTempResultList = glob.iglob(r'%s' %
                                              sessionMemTempResultPattern)
        for tempFile in sessionMemTempResultList:
            os.remove(tempFile)

        sessionIOTempResultPattern = os.path.join(
            DefaultValue.getTmpDirFromEnv(), 'sessionIOTempResult_*_*')
        sessionIOTempResultList = glob.iglob(r'%s' %
                                             sessionIOTempResultPattern)
        for tempFile in sessionIOTempResultList:
            os.remove(tempFile)

    def collect(self):
        '''
        function: collect performance statistics
        input : NA
        output: NA
        '''
        try:
            self.logger.debug("Collecting performance statistics.")
            self.cleanTempFiles()

            # get pgxc node of the cluster
            pgxcNodeList = self.getPGXCNode()

            self.logger.debug("pgxcNodeList: %s" % pgxcNodeList)

            # collect PGXC node performance statistics
            self.collectPGXCNodeStat(pgxcNodeList)

            if (not dwsFlag):
                # collect session cpu performance statistics
                self.collectSessionCpuStatNew(pgxcNodeList)
                # collect session memory performance statistics
                self.collectSessionMemStatNew(pgxcNodeList)
                # collect session io performance statistics
                self.collectSessionIOStatNew(pgxcNodeList)

            self.logger.debug("Successfully collected performance statistics.")

        except Exception as e:
            raise Exception(str(e))

    def display(self):
        '''
        function: display performance statistics
        input : NA
        output: NA
        '''
        try:
            self.logger.debug("Displaying performance statistics.")

            # clean all the temp files before display the performance data
            queryTempFilePattern = os.path.join(
                DefaultValue.getTmpDirFromEnv(),
                'checkperf_query_*_*')
            queryTempFileList = glob.iglob(r'%s' % queryTempFilePattern)
            for tempFile in queryTempFileList:
                os.remove(tempFile)

            actionList = ["ClusterHostCpuStat",
                          "ClusterMPPDBCpuStat",
                          "ShareBufferStat",
                          "ClusterSortStat",
                          "ClusterIOStat",
                          "ClusterDiskStat",
                          "ClusterActiveSqlCount",
                          "ClusterSessionCount",
                          "NodeCpuStat",
                          "NodeMemoryStat",
                          "NodeIOStat"]

            sessionList = ["SessionCpuStat",
                           "SessionMemoryStat",
                           "SessionIOStat"]

            if (not dwsFlag):
                actionList.extend(sessionList)
            # Concurrent execute collectStat function
            pool = ThreadPool(DEFAULT_PARALLEL_NUM)
            results = pool.map(self.collectStat, actionList)
            pool.close()
            pool.join()
            self.outPut()
            self.logger.debug("Successfully displayed performance statistics.")
        except Exception as e:
            raise Exception(str(e))

    def asynCollectDatabaseSize(self, nodePort):
        """
        function: asyn collect database size
        input : NA
        output: NA
        """
        self.logger.debug("Asyn collecting database size on current node.")

        try:
            querySql = "SELECT 'total_database_size:' || " \
                       "SUM(pg_database_size(oid))::bigint FROM pg_database;"
            if (g_DWS_mode):
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(nodePort, querySql)
                self.logger.debug("Asyn collecting database size. "
                                  "\ncommand: %s \nresult: %s." %
                                  (querySql, result))

                if (status != 2 or err_output.strip() != ""):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % err_output)

                binPath = os.path.join(g_clusterInfo.appPath, "bin")
                databaseSizeFile = os.path.join(binPath,
                                                DefaultValue.DB_SIZE_FILE)
                output = result[0][0]

                g_file.createFileInSafeMode(databaseSizeFile)
                with open(databaseSizeFile, 'w') as f:
                    f.writelines(output)
                    if (f):
                        f.close()
                tmp_sshTool = SshTool(g_clusterInfo.getClusterNodeNames(),
                                      self.logger.logFile)
                tmp_sshTool.scpFiles(databaseSizeFile, binPath)
            else:
                (status, output) = ClusterCommand.execSQLCommand(querySql,
                                                                 self.user,
                                                                 '', nodePort,
                                                                 "postgres")
                self.logger.debug("Asyn collecting database size. \ncommand: "
                                  "%s \nresult: %s." % (querySql, output))

                if (status != 0 or output.strip() == ""):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] %
                                    querySql + " Error:\n%s" % output)

                binPath = os.path.join(g_clusterInfo.appPath, "bin")
                databaseSizeFile = os.path.join(binPath,
                                                DefaultValue.DB_SIZE_FILE)
                g_file.createFileInSafeMode(databaseSizeFile)
                with open(databaseSizeFile, 'w') as f:
                    f.writelines(output)
                    if (f):
                        f.close()
                tmp_sshTool = SshTool(g_clusterInfo.getClusterNodeNames(),
                                      self.logger.logFile)
                tmp_sshTool.scpFiles(databaseSizeFile, binPath)
        except Exception as e:
            raise Exception(str(e))

        self.logger.debug("Successfully asyn collected database size"
                          " on current node.")

    def outPut(self):
        '''
        function: output statistics
        input : NA
        output: NA
        '''
        try:
            # judge if enter parameter '--detail'
            if self.showDetail:
                # detail display result
                self.detailDisplay()
            else:
                # summary display result
                self.summaryDisplay()
        except Exception as e:
            raise Exception(str(e))

    def collectClusterHostCpuStat(self):
        '''
        function: collect cluster host CPU statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_avg_cpu_total_time, o_avg_cpu_busy_time, " \
              "o_avg_cpu_iowait_time, o_cpu_busy_perc, o_cpu_io_wait_perc "
        sql += "FROM pmk.get_cluster_host_cpu_stat(null, null);"

        self.logger.debug("Collecting cluster host CPU statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command

                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_host_total_cpu_time = \
                    statItem(items[0], "Jiffies")
                self.cluster_stat.cluster_host_cpu_busy_time = \
                    statItem(items[1], "Jiffies")
                self.cluster_stat.cluster_host_cpu_iowait_time = \
                    statItem(items[2], "Jiffies")
                self.cluster_stat.cluster_host_cpu_busy_time_perc = \
                    statItem(items[3], "%")
                self.cluster_stat.cluster_host_cpu_iowait_time_perc = \
                    statItem(items[4], "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, " % (self.__baselineFlag)
            sql += "o_avg_cpu_total_time, o_avg_cpu_busy_time, " \
                   "o_avg_cpu_iowait_time, o_cpu_busy_perc, " \
                   "o_cpu_io_wait_perc "
            sql += "FROM pmk.get_cluster_host_cpu_stat(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 6, 1)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_host_total_cpu_time = \
                    statItem(items[1], "Jiffies")
                self.cluster_stat.cluster_host_cpu_busy_time = \
                    statItem(items[2], "Jiffies")
                self.cluster_stat.cluster_host_cpu_iowait_time = \
                    statItem(items[3], "Jiffies")
                self.cluster_stat.cluster_host_cpu_busy_time_perc = \
                    statItem(items[4], "%")
                self.cluster_stat.cluster_host_cpu_iowait_time_perc = \
                    statItem(items[5], "%")

        self.logger.debug("Successfully collected cluster host CPU state.")

    def collectClusterMPPDBCpuStat(self):
        '''
        function: collect MPPDB CPU statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_mppdb_cpu_time_perc_wrt_busy_time, " \
              "o_mppdb_cpu_time_perc_wrt_total_time FROM "
        sql += "pmk.get_cluster_mppdb_cpu_stat(null, null);"

        self.logger.debug("Collecting MPPDB CPU statistics.")
        if (g_DWS_mode):
            try:
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time = \
                    statItem(items[0], "%")
                self.cluster_stat.cluster_mppdb_cpu_time_in_total_time = \
                    statItem(items[1], "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, " \
                  "o_mppdb_cpu_time_perc_wrt_busy_time, " \
                  "o_mppdb_cpu_time_perc_wrt_total_time FROM " % \
                  (self.__baselineFlag)
            sql += "pmk.get_cluster_mppdb_cpu_stat(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 3, 2)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time = \
                    statItem(items[1], "%")
                self.cluster_stat.cluster_mppdb_cpu_time_in_total_time = \
                    statItem(items[2], "%")
        self.logger.debug(
            "Successfully collected cluster MPPDB CPU statistics.")

    def collectShareBufferStat(self):
        '''
        function: collect share buffer statistics
        input : NA
        output: NA
        '''
        sql = "SELECT  o_total_blocks_read, o_total_blocks_hit, " \
              "o_shared_buffer_hit_ratio "
        sql += "FROM pmk.get_cluster_shared_buffer_stat(null, null);"

        self.logger.debug("Collecting shared buffer statistics.")
        if (g_DWS_mode):
            try:
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_536["GAUSS_53611"] % str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_share_buffer_read = \
                    statItem(items[0])
                self.cluster_stat.cluster_share_buffer_hit = statItem(items[1])
                self.cluster_stat.cluster_share_buffer_hit_ratio = \
                    statItem(items[2], "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, o_total_blocks_read, " \
                  "o_total_blocks_hit, o_shared_buffer_hit_ratio " % \
                  (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_shared_buffer_stat(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 4, 3)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_share_buffer_read = \
                    statItem(items[1])
                self.cluster_stat.cluster_share_buffer_hit = statItem(items[2])
                self.cluster_stat.cluster_share_buffer_hit_ratio = \
                    statItem(items[3], "%")
        self.logger.debug("Successfully collected shared buffer statistics.")

    def collectClusterSortStat(self):
        '''
        function: collect sort statistics
        input : NA
        output: NA 
        '''
        sql = "SELECT o_total_memory_sorts, o_total_disk_sorts, " \
              "o_memory_sort_ratio "
        sql += "FROM pmk.get_cluster_memory_sort_stat(null, null);"

        self.logger.debug("Collecting sort statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_in_memory_sort_count = \
                    statItem(items[0])
                self.cluster_stat.cluster_disk_sort_count = statItem(items[1])
                self.cluster_stat.cluster_in_memory_sort_ratio = \
                    statItem(items[2], "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, o_total_memory_sorts, " \
                  "o_total_disk_sorts, o_memory_sort_ratio " % \
                  (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_memory_sort_stat(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 4, 4)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_in_memory_sort_count = \
                    statItem(items[1])
                self.cluster_stat.cluster_disk_sort_count = statItem(items[2])
                self.cluster_stat.cluster_in_memory_sort_ratio = \
                    statItem(items[3], "%")
        self.logger.debug("Successfully collected cluster sort statistics.")

    def collectClusterIOStat(self):
        '''
        function: collect IO statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_number_of_files, o_physical_reads, " \
              "o_physical_writes, " \
              "o_read_time, o_write_time "
        sql += "FROM pmk.get_cluster_io_stat(null, null);"

        self.logger.debug("Collecting IO statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_io_stat_number_of_files = \
                    statItem(items[0])
                self.cluster_stat.cluster_io_stat_physical_reads = \
                    statItem(items[1])
                self.cluster_stat.cluster_io_stat_physical_writes = \
                    statItem(items[2])
                self.cluster_stat.cluster_io_stat_read_time = \
                    statItem(items[3], "ms")
                self.cluster_stat.cluster_io_stat_write_time = \
                    statItem(items[4], "ms")
        else:
            sql = "SELECT o_stat_collect_time as %s, o_number_of_files, " \
                  "o_physical_reads, o_physical_writes, o_read_time, " \
                  "o_write_time " % (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_io_stat(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 6, 5)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_io_stat_number_of_files = \
                    statItem(items[1])
                self.cluster_stat.cluster_io_stat_physical_reads = \
                    statItem(items[2])
                self.cluster_stat.cluster_io_stat_physical_writes = \
                    statItem(items[3])
                self.cluster_stat.cluster_io_stat_read_time = \
                    statItem(items[4], "ms")
                self.cluster_stat.cluster_io_stat_write_time = \
                    statItem(items[5], "ms")
        self.logger.debug("Successfully collected cluster IO statistics.")

    def collectClusterDiskStat(self):
        '''
        function: collect disk usage statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_tot_datanode_db_size, o_tot_physical_writes, " \
              "o_avg_write_per_sec, o_max_node_physical_writes "
        sql += "FROM pmk.get_cluster_disk_usage_stat(null, null, '%s');" % \
               str(database_size)

        self.logger.debug("Collecting disk usage statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_disk_usage_db_size = \
                    statItem(items[0].split()[0], items[0].split()[1])
                self.cluster_stat.cluster_disk_usage_tot_physical_writes = \
                    statItem(items[1])
                self.cluster_stat.cluster_disk_usage_avg_physical_write = \
                    statItem(items[2])
                self.cluster_stat.cluster_disk_usage_max_physical_write = \
                    statItem(items[3])
        else:
            sql = "SELECT o_stat_collect_time as %s, o_tot_datanode_db_size," \
                  " o_tot_physical_writes, o_avg_write_per_sec, " \
                  "o_max_node_physical_writes " % (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_disk_usage_stat(null, null, '%s');" \
                   % str(database_size)
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 5, 6)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_disk_usage_db_size = \
                    statItem(items[1].split()[0], items[1].split()[1])
                self.cluster_stat.cluster_disk_usage_tot_physical_writes = \
                    statItem(items[2])
                self.cluster_stat.cluster_disk_usage_avg_physical_write = \
                    statItem(items[3])
                self.cluster_stat.cluster_disk_usage_max_physical_write = \
                    statItem(items[4])
        self.logger.debug("Successfully collected cluster disk statistics.")

    def collectClusterActiveSqlCount(self):
        '''
        function: collect active SQL statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_tot_active_sql_count "
        sql += "FROM pmk.get_cluster_active_sql_count(null, null);"

        self.logger.debug("Collecting active SQL statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_activity_active_sql_count = \
                    statItem(items[0])
        else:
            sql = "SELECT o_stat_collect_time as %s, o_tot_active_sql_count " \
                  "" % \
                  (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_active_sql_count(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 2, 7)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_activity_active_sql_count = \
                    statItem(items[1])
        self.logger.debug("Successfully collected cluster active SQL count.")

    def collectClusterSessionCount(self):
        '''
        function: collect session count statistics
        input : NA
        output: NA 
        '''
        sql = "SELECT o_tot_session_count "
        sql += "FROM pmk.get_cluster_session_count(null, null);"

        self.logger.debug("Collecting session count statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(result) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(result))
            items = result[0]
            if (items is not None):
                self.cluster_stat.cluster_activity_session_count = \
                    statItem(items[0])
        else:
            sql = "SELECT o_stat_collect_time as %s, o_tot_session_count " % \
                  (self.__baselineFlag)
            sql += "FROM pmk.get_cluster_session_count(null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 2, 8)
            except Exception as e:
                raise Exception(str(e))
            # failed to execute the sql command
            if (len(records) != 1):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error number: %d." % len(records))
            items = records[0]
            if (items is not None):
                self.cluster_stat.cluster_activity_session_count = \
                    statItem(items[1])
        self.logger.debug("Successfully collected cluster session count.")

    def collectNodeCpuStat(self):
        ''' 
        function: collect node CPU statistics 
        input : NA
        output: NA
        '''
        sql = "SELECT o_node_name, "
        sql += "o_mppdb_cpu_time, o_host_cpu_busy_time, " \
               "o_host_cpu_total_time, " \
               "o_mppdb_cpu_time_perc_wrt_busy_time, "
        sql += "o_mppdb_cpu_time_perc_wrt_total_time FROM " \
               "pmk.get_node_cpu_stat('all', null, null);"

        self.logger.debug("Collecting node CPU statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            recordsCount = len(result)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = result[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[0].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[0].strip())
                    self.node_stat.append(node)

                node.node_mppdb_cpu_busy_time = statItem(record[1], "Jiffies")
                node.node_host_cpu_busy_time = statItem(record[2], "Jiffies")
                node.node_host_cpu_total_time = statItem(record[3], "Jiffies")
                node.node_mppdb_cpu_time_in_busy_time = statItem(record[4],
                                                                 "%")
                node.node_mppdb_cpu_time_in_total_time = statItem(record[5],
                                                                  "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, o_node_name, " % \
                  (self.__baselineFlag)
            sql += "o_mppdb_cpu_time, o_host_cpu_busy_time, " \
                   "o_host_cpu_total_time, " \
                   "o_mppdb_cpu_time_perc_wrt_busy_time, "
            sql += "o_mppdb_cpu_time_perc_wrt_total_time FROM " \
                   "pmk.get_node_cpu_stat('all', null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 7, 9)
            except Exception as e:
                raise Exception(str(e))

            recordsCount = len(records)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = records[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[1].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[1].strip())
                    self.node_stat.append(node)

                node.node_mppdb_cpu_busy_time = statItem(record[2], "Jiffies")
                node.node_host_cpu_busy_time = statItem(record[3], "Jiffies")
                node.node_host_cpu_total_time = statItem(record[4], "Jiffies")
                node.node_mppdb_cpu_time_in_busy_time = \
                    statItem(record[5], "%")
                node.node_mppdb_cpu_time_in_total_time = \
                    statItem(record[6], "%")
        self.logger.debug("Successfully collected node CPU statistics.")

    def collectNodeMemoryStat(self):
        '''
        function: collect node memory statistics 
        input : NA
        output: NA
        '''
        sql = "SELECT o_node_name, "
        sql += "o_physical_memory, o_shared_buffer_size, " \
               "o_shared_buffer_hit_ratio, o_sorts_in_memory, "
        sql += "o_sorts_in_disk, o_in_memory_sort_ratio, o_db_memory_usage " \
               "FROM pmk.get_node_memory_stat('all', null, null);"

        self.logger.debug("Collecting node memory statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))
            recordsCount = len(result)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = result[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[0].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[0].strip())
                    self.node_stat.append(node)

                node.node_physical_memory = statItem(record[1], "Bytes")
                node.node_db_memory_usage = statItem(record[7], "Bytes")
                node.node_shared_buffer_size = statItem(record[2], "Bytes")
                node.node_shared_buffer_hit_ratio = statItem(record[3], "%")
                node.node_in_memory_sorts = statItem(record[4], )
                node.node_in_disk_sorts = statItem(record[5], )
                node.node_in_memory_sort_ratio = statItem(record[6], "%")
        else:
            sql = "SELECT o_stat_collect_time as %s, o_node_name, " % \
                  (self.__baselineFlag)
            sql += "o_physical_memory, o_shared_buffer_size, " \
                   "o_shared_buffer_hit_ratio, o_sorts_in_memory, "
            sql += "o_sorts_in_disk, o_in_memory_sort_ratio, " \
                   "o_db_memory_usage " \
                   "FROM pmk.get_node_memory_stat('all', null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 9, 10)
            except Exception as e:
                raise Exception(str(e))

            recordsCount = len(records)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = records[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[1].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[1].strip())
                    self.node_stat.append(node)

                node.node_physical_memory = statItem(record[2], "Bytes")
                node.node_db_memory_usage = statItem(record[8], "Bytes")
                node.node_shared_buffer_size = statItem(record[3], "Bytes")
                node.node_shared_buffer_hit_ratio = statItem(record[4], "%")
                node.node_in_memory_sorts = statItem(record[5], )
                node.node_in_disk_sorts = statItem(record[6], )
                node.node_in_memory_sort_ratio = statItem(record[7], "%")
        self.logger.debug("Successfully collected node memory statistics.")

    def collectNodeIOStat(self):
        '''
        function: collect node IO statistics
        input : NA
        output: NA
        '''
        sql = "SELECT o_node_name, "
        sql += "o_number_of_files, o_physical_reads, o_physical_writes, " \
               "o_read_time, "
        sql += "o_write_time FROM pmk.get_node_io_stat('all', null, null);"

        self.logger.debug("Collecting node IO statistics.")
        if (g_DWS_mode):
            try:
                # execute the sql command
                (status, result, err_output) = \
                    ClusterCommand.excuteSqlOnLocalhost(self.localport, sql)
            except Exception as e:
                raise Exception(str(e))

            recordsCount = len(result)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = result[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[0].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[0].strip())
                    self.node_stat.append(node)

                node.node_number_of_files = statItem(record[1])
                node.node_physical_reads = statItem(record[2])
                node.node_physical_writes = statItem(record[3])
                node.node_read_time = statItem(record[4])
                node.node_write_time = statItem(record[5])
        else:
            sql = "SELECT o_stat_collect_time as %s, o_node_name, " % \
                  (self.__baselineFlag)
            sql += "o_number_of_files, o_physical_reads, o_physical_writes, " \
                   "o_read_time, "
            sql += "o_write_time FROM pmk.get_node_io_stat('all', null, null);"
            try:
                # execute the sql command
                records = self.execQueryCommand(sql, 7, 11)
            except Exception as e:
                raise Exception(str(e))

            recordsCount = len(records)
            # failed to execute the sql command
            if (recordsCount == 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % sql +
                                " Error: No records returned.")

            for i in range(0, recordsCount):
                record = records[i]

                found = False
                for node in self.node_stat:
                    if (node.nodename == record[1].strip()):
                        found = True
                        break

                if (not found):
                    node = nodeStatistics(record[1].strip())
                    self.node_stat.append(node)

                node.node_number_of_files = statItem(record[2])
                node.node_physical_reads = statItem(record[3])
                node.node_physical_writes = statItem(record[4])
                node.node_read_time = statItem(record[5])
                node.node_write_time = statItem(record[6])
        self.logger.debug("Successfully collected node IO statistics.")

    def collectSessionCpuStat(self):
        '''
        function: collect session CPU statistics
        input : NA
        output: NA
        '''
        self.logger.debug("Collecting session CPU statistics.")
        sessionCpuTempResult = ""
        try:
            # get session Cpu Temp Result
            sessionCpuTempResult = os.path.join(
                DefaultValue.getTmpDirFromEnv(),
                "sessionCpuTempResult_%d_%s" % \
                (self.flagNum, self.masterHost))
            # read session Cpu Temp Result
            with open(sessionCpuTempResult, 'r') as fp:
                # parse session Cpu Temp Result
                for line in fp.readlines():
                    line = line.strip()
                    if line != "":
                        tempList = line.split('|')
                        sess = sessionStatistics((tempList[0]).strip(),
                                                 (tempList[1]).strip(),
                                                 (tempList[2]).strip())
                        sess.session_cpu_time = statItem((tempList[3]).strip())
                        sess.session_db_cpu_time = \
                            statItem((tempList[4]).strip())
                        sess.session_cpu_percent = \
                            statItem((tempList[5]).strip(), "%")
                        self.session_cpu_stat.append(sess)
            # close and remove session Cpu Temp Result
            DefaultValue.cleanTmpFile(sessionCpuTempResult)
        except Exception as e:
            # close and remove session Cpu Temp Result
            DefaultValue.cleanTmpFile(sessionCpuTempResult)
            raise Exception(str(e))
        self.logger.debug("Successfully collected session CPU statistics.")

    def collectSessionMemoryStat(self):
        '''
        function: collect session memory statistics
        input : NA
        output: NA
        '''
        self.logger.debug("Collecting session memory statistics.")
        sessionMemTempResult = ""
        try:
            # get session Memory Temp Result
            sessionMemTempResult = os.path.join(
                DefaultValue.getTmpDirFromEnv(),
                "sessionMemTempResult_%d_%s" % \
                (self.flagNum, self.masterHost))
            # read session Memory Temp Result
            with open(sessionMemTempResult, 'r') as fp:
                # parse session Memory Temp Result
                for line in fp.readlines():
                    line = line.strip()
                    if line != "":
                        tempList = line.split('|')
                        sess = sessionStatistics((tempList[0]).strip(),
                                                 (tempList[1]).strip(),
                                                 (tempList[2]).strip())
                        sess.session_buffer_reads = \
                            statItem((tempList[5]).strip())
                        sess.session_buffer_hit_ratio = \
                            statItem((tempList[6]).strip())
                        sess.session_in_memory_sorts = \
                            statItem((tempList[7]).strip())
                        sess.session_in_disk_sorts = statItem(
                            (tempList[8]).strip())
                        sess.session_in_memory_sorts_ratio = \
                            statItem((tempList[9]).strip())
                        sess.session_total_memory_size = \
                            statItem((tempList[3]).strip())
                        sess.session_used_memory_size = \
                            statItem((tempList[4]).strip())
                        self.session_mem_stat.append(sess)
            # close and remove session Memory Temp Result
            DefaultValue.cleanTmpFile(sessionMemTempResult)
        except Exception as e:
            # close and remove session Memory Temp Result
            DefaultValue.cleanTmpFile(sessionMemTempResult)
            raise Exception(str(e))
        self.logger.debug("Successfully collected session memory statistics.")

    def collectSessionIOStat(self):
        '''
        function: collect session IO statistics
        input : NA
        output: NA
        '''
        self.logger.debug("Collecting session IO statistics.")
        sessionIOTempResult = ""
        try:
            # get session IO Temp Result
            sessionIOTempResult = os.path.join(DefaultValue.getTmpDirFromEnv(),
                                               "sessionIOTempResult_%d_%s" % \
                                               (self.flagNum, self.masterHost))
            # read session IO Temp Result
            with open(sessionIOTempResult, 'r') as fp:
                # parse session IO Temp Result
                for line in fp.readlines():
                    line = line.strip()
                    if line != "":
                        tempList = line.split('|')
                        sess = sessionStatistics((tempList[0]).strip(),
                                                 (tempList[1]).strip(),
                                                 (tempList[2]).strip())
                        sess.session_physical_reads = \
                            statItem((tempList[3]).strip())
                        sess.session_read_time = \
                            statItem((tempList[4]).strip())
                        self.session_io_stat.append(sess)
            # close and remove session IO Temp Result
            DefaultValue.cleanTmpFile(sessionIOTempResult)
        except Exception as e:
            # close and remove session IO Temp Result
            DefaultValue.cleanTmpFile(sessionIOTempResult)
            raise Exception(str(e))
        self.logger.debug("Successfully collected session IO statistics.")

    def displayOneStatItem(self, desc, disvalue):
        '''
        function: display one statistic item
        input : desc, disvalue
        output: NA
        '''
        # judge if disvalue is none
        if (str(disvalue) != ""):
            self.writeOutput("    %-45s:    %s" % (desc, str(disvalue)))
        else:
            self.writeOutput("    %-45s:    0" % (desc))

    def summaryDisplay(self):
        '''
        function: summary display
        input : NA
        output: NA
        '''
        # show cluster statistics summary information
        self.writeOutput("Cluster statistics information:")
        # show host CPU busy time ratio
        self.displayOneStatItem(
            "Host CPU busy time ratio",
            self.cluster_stat.cluster_host_cpu_busy_time_perc)
        # show MPPDB CPU time
        self.displayOneStatItem(
            "MPPDB CPU time % in busy time",
            self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time)
        # show shared buffer hit ratio
        self.displayOneStatItem(
            "Shared Buffer Hit ratio",
            self.cluster_stat.cluster_share_buffer_hit_ratio)
        # show In-memory sort ratio
        self.displayOneStatItem(
            "In-memory sort ratio",
            self.cluster_stat.cluster_in_memory_sort_ratio)
        # show physical reads
        self.displayOneStatItem(
            "Physical Reads",
            self.cluster_stat.cluster_io_stat_physical_reads)
        # show physical writes
        self.displayOneStatItem(
            "Physical Writes",
            self.cluster_stat.cluster_io_stat_physical_writes)
        # show DB size
        self.displayOneStatItem(
            "DB size",
            self.cluster_stat.cluster_disk_usage_db_size)
        # show Total Physical writes
        self.displayOneStatItem(
            "Total Physical writes",
            self.cluster_stat.cluster_disk_usage_tot_physical_writes)
        # show Active SQL count
        self.displayOneStatItem(
            "Active SQL count",
            self.cluster_stat.cluster_activity_active_sql_count)
        # show Session count
        self.displayOneStatItem(
            "Session count",
            self.cluster_stat.cluster_activity_session_count)

    def detailDisplay(self):
        '''
        function: detail display
        input : NA
        output: NA
        '''
        # show host CPU ratio in detail
        self.writeOutput("Cluster statistics information:")
        self.writeOutput("Host CPU usage rate:")
        self.displayOneStatItem(
            "Host total CPU time",
            self.cluster_stat.cluster_host_total_cpu_time)
        self.displayOneStatItem(
            "Host CPU busy time",
            self.cluster_stat.cluster_host_cpu_busy_time)
        self.displayOneStatItem(
            "Host CPU iowait time",
            self.cluster_stat.cluster_host_cpu_iowait_time)
        self.displayOneStatItem(
            "Host CPU busy time ratio",
            self.cluster_stat.cluster_host_cpu_busy_time_perc)
        self.displayOneStatItem(
            "Host CPU iowait time ratio",
            self.cluster_stat.cluster_host_cpu_iowait_time_perc)

        # show MPPDB CPU time in detail
        self.writeOutput("MPPDB CPU usage rate:")
        self.displayOneStatItem(
            "MPPDB CPU time % in busy time",
            self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time)
        self.displayOneStatItem(
            "MPPDB CPU time % in total time",
            self.cluster_stat.cluster_mppdb_cpu_time_in_total_time)

        # show Shared Buffer Hit ratio in detail
        self.writeOutput("Shared buffer hit rate:")
        self.displayOneStatItem("Shared Buffer Reads",
                                self.cluster_stat.cluster_share_buffer_read)
        self.displayOneStatItem("Shared Buffer Hits",
                                self.cluster_stat.cluster_share_buffer_hit)
        self.displayOneStatItem(
            "Shared Buffer Hit ratio",
            self.cluster_stat.cluster_share_buffer_hit_ratio)

        # show In-memory sort ratio in detail
        self.writeOutput("In memory sort rate:")
        self.displayOneStatItem("In-memory sort count",
                                self.cluster_stat.cluster_in_memory_sort_count)
        self.displayOneStatItem("In-disk sort count",
                                self.cluster_stat.cluster_disk_sort_count)
        self.displayOneStatItem("In-memory sort ratio",
                                self.cluster_stat.cluster_in_memory_sort_ratio)

        # show I/O usage in detail
        self.writeOutput("I/O usage:")
        self.displayOneStatItem(
            "Number of files",
            self.cluster_stat.cluster_io_stat_number_of_files)
        self.displayOneStatItem(
            "Physical Reads",
            self.cluster_stat.cluster_io_stat_physical_reads)
        self.displayOneStatItem(
            "Physical Writes",
            self.cluster_stat.cluster_io_stat_physical_writes)
        self.displayOneStatItem("Read Time",
                                self.cluster_stat.cluster_io_stat_read_time)
        self.displayOneStatItem("Write Time",
                                self.cluster_stat.cluster_io_stat_write_time)

        # show Disk usage in detail
        self.writeOutput("Disk usage:")
        self.displayOneStatItem("DB size",
                                self.cluster_stat.cluster_disk_usage_db_size)
        self.displayOneStatItem(
            "Total Physical writes",
            self.cluster_stat.cluster_disk_usage_tot_physical_writes)
        self.displayOneStatItem(
            "Average Physical write",
            self.cluster_stat.cluster_disk_usage_avg_physical_write)
        self.displayOneStatItem(
            "Maximum Physical write",
            self.cluster_stat.cluster_disk_usage_max_physical_write)

        # show SQL count and session count in detail
        self.writeOutput("Activity statistics:")
        self.displayOneStatItem(
            "Active SQL count",
            self.cluster_stat.cluster_activity_active_sql_count)
        self.displayOneStatItem(
            "Session count",
            self.cluster_stat.cluster_activity_session_count)

        # show node statistics information
        self.writeOutput("Node statistics information:")
        for node in self.node_stat:
            # show node name
            self.writeOutput("%s:" % node.nodename)
            # show cpu usage in host
            self.displayOneStatItem("MPPDB CPU Time",
                                    node.node_mppdb_cpu_busy_time)
            self.displayOneStatItem("Host CPU Busy Time",
                                    node.node_host_cpu_busy_time)
            self.displayOneStatItem("Host CPU Total Time",
                                    node.node_host_cpu_total_time)
            self.displayOneStatItem("MPPDB CPU Time % in Busy Time",
                                    node.node_mppdb_cpu_time_in_busy_time)
            self.displayOneStatItem("MPPDB CPU Time % in Total Time",
                                    node.node_mppdb_cpu_time_in_total_time)

            # show memory usage in host
            self.displayOneStatItem("Physical memory",
                                    node.node_physical_memory)
            self.displayOneStatItem("DB Memory usage",
                                    node.node_db_memory_usage)
            self.displayOneStatItem("Shared buffer size",
                                    node.node_shared_buffer_size)
            self.displayOneStatItem("Shared buffer hit ratio",
                                    node.node_shared_buffer_hit_ratio)
            self.displayOneStatItem("Sorts in memory",
                                    node.node_in_memory_sorts)
            self.displayOneStatItem("Sorts in disk",
                                    node.node_in_disk_sorts)
            self.displayOneStatItem("In-memory sort ratio",
                                    node.node_in_memory_sort_ratio)

            # show IO usage in host
            self.displayOneStatItem("Number of files",
                                    node.node_number_of_files)
            self.displayOneStatItem("Physical Reads",
                                    node.node_physical_reads)
            self.displayOneStatItem("Physical Writes",
                                    node.node_physical_writes)
            self.displayOneStatItem("Read Time",
                                    node.node_read_time)
            self.displayOneStatItem("Write Time",
                                    node.node_write_time)

        # show session statistics information
        self.writeOutput("Session statistics information(Top %d):" %
                         self.__TopNSessions)
        # show session cpu usage statistics
        self.writeOutput("Session CPU statistics:")
        for i in range(0, len(self.session_cpu_stat)):
            sess = self.session_cpu_stat[i]
            self.writeOutput("%d %s-%s-%s:" %
                             (
                                 i + 1, sess.nodename, sess.dbname,
                                 sess.username))
            self.displayOneStatItem("Session CPU time",
                                    sess.session_cpu_time)
            self.displayOneStatItem("Database CPU time",
                                    sess.session_db_cpu_time)
            self.displayOneStatItem("Session CPU time %",
                                    sess.session_cpu_percent)

        # show session Memory statistics
        self.writeOutput("\nSession Memory statistics:")
        for i in range(0, len(self.session_mem_stat)):
            sess = self.session_mem_stat[i]
            self.writeOutput("%d %s-%s-%s:" %
                             (
                                 i + 1, sess.nodename, sess.dbname,
                                 sess.username))
            self.displayOneStatItem("Buffer Reads", sess.session_buffer_reads)
            self.displayOneStatItem("Shared Buffer Hit ratio",
                                    sess.session_buffer_hit_ratio)
            self.displayOneStatItem("In Memory sorts",
                                    sess.session_in_memory_sorts)
            self.displayOneStatItem("In Disk sorts",
                                    sess.session_in_disk_sorts)
            self.displayOneStatItem("In Memory sorts ratio",
                                    sess.session_in_memory_sorts_ratio)
            self.displayOneStatItem("Total Memory Size",
                                    sess.session_total_memory_size)
            self.displayOneStatItem("Used Memory Size",
                                    sess.session_used_memory_size)

        # show session IO statistics
        self.writeOutput("\nSession IO statistics:")
        for i in range(0, len(self.session_io_stat)):
            sess = self.session_io_stat[i]
            self.writeOutput("%d %s-%s-%s:" %
                             (
                                 i + 1, sess.nodename, sess.dbname,
                                 sess.username))
            self.displayOneStatItem("Physical Reads",
                                    sess.session_physical_reads)
            self.displayOneStatItem("Read Time", sess.session_read_time)


if __name__ == '__main__':

    import getopt

    sys.path.append(sys.path[0] + "/../../")
    from gspylib.common.GaussLog import GaussLog


    def usage():
        """
Usage:
    python3 GaussStat.py -p installpath -u user -c ip:port [-f output] [-d]
    [-l log]
    
options:      
    -p                                Install path.
    -u                                Database user name.
    -c                                Host information.
    -d --detail                       Show the detail info about
                                      performance check.
    -l --logpath=logfile              The log file of operation.
    -h --help                         Show this help, then exit.
        """
        print(usage.__doc__)


    # parse parameters from command line
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:p:u:c:l:dh",
                                     ["logpath=", "detail", "help", "dws-mode",
                                      "curr-time=", "last-time=",
                                      "snapshot-id=", "flag-num=",
                                      "master-host=", "database-size=",
                                      "abnormal-CN="])
    except Exception as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] %
                               str(args[0]))

    # state variable
    installPath = ""
    user = ""
    logFile = ""
    localPort = []
    detail = False
    currTime = ""
    lastTime = ""
    snapshotId = ""
    flagNum = 0
    masterHost = ""
    action = ""
    dwsFlag = False
    database_size = 0
    abnormalCN = []

    # get parameter value
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            action = value.strip()
        elif (key == "-p"):
            installPath = value.strip()
        elif (key == "-u"):
            user = value.strip()
        elif (key == "-c"):
            localPort = value.strip()
        elif (key == "-l" or key == "--logpath"):
            logFile = value.strip()
        elif (key == "-d" or key == "--detail"):
            detail = True
        elif (key == "--curr-time"):
            currTime = value.strip()
        elif (key == "--last-time"):
            lastTime = value.strip()
        elif (key == "--snapshot-id"):
            snapshotId = value.strip()
        elif (key == "--flag-num"):
            flagNum = value
        elif (key == "--master-host"):
            masterHost = value.strip()
        elif (key == "--dws-mode"):
            dwsFlag = True
        elif (key == "--database-size"):
            database_size = int(value)
        elif (key == "--abnormal-CN"):
            abnormalCN = str(value).split(",")

    # judge if install path exists or user or local port is none
    if (not os.path.exists(installPath) or user == "" or localPort == ""):
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] %
                               "p or -u or -c" + ".")

    # get log file
    if (logFile == ""):
        logFile = "%s/om/%s" % (DefaultValue.getUserLogDirWithUser(user),
                                DefaultValue.LOCAL_LOG_FILE)

    # initialize log
    logger = GaussLog(logFile, "GaussStat")

    try:
        g_clusterInfo = dbClusterInfo()
        # Init cluster from static configuration file
        g_clusterInfo.initFromStaticConfig(user)
        localNodeInfo = g_clusterInfo.getDbNodeByName(
            DefaultValue.GetHostIpOrName())
        security_mode_value = DefaultValue.getSecurityMode()
        if (security_mode_value == "on"):
            g_DWS_mode = True
        stat = GaussStat(installPath, user, localPort, currTime, lastTime,
                         snapshotId, int(flagNum), masterHost, logger,
                         detail)
        if (action == ACTION_INSTALL_PMK):
            # install PMK shcema
            stat.installPMKSchema()
        elif (action == ACTION_COLLECT_STAT):
            # collect performance statistics
            stat.collect()
        elif (action == ACTION_DISPLAY_STAT):
            # display performance statistics
            stat.display()
        elif (action == ACTION_ASYN_COLLECT):
            # asyn collect database size
            stat.asynCollectDatabaseSize(localPort)
    except Exception as e:
        logger.logExit(str(e))
    # close log file
    logger.closeLog()
