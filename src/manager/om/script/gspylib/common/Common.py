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
# Description  : Common is a utility with a lot of common functions
#############################################################################
import sys
import subprocess
import os
import platform
import socket
import types
import re
import time
import configparser
import multiprocessing
import _thread as thread
import pwd
import base64
import struct
import binascii
import json

# The installation starts, but the package is not decompressed completely.
# The lib64/libz.so.1 file is incomplete, and the hashlib depends on the
# libz.so.1 file.
num = 0
while num < 10:
    try:
        import hashlib

        break
    except ImportError:
        num += 1
        time.sleep(1)

from random import sample
import csv
import shutil
import string
import traceback
from ctypes import *
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime

localDirPath = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, localDirPath + "/../../../lib")
try:
    import psutil
except ImportError as e:
    # mv psutil mode .so file by python version
    pythonVer = sys.version[:3]
    psutilLinux = os.path.join(localDirPath,
                               "./../../../lib/psutil/_psutil_linux.so")
    psutilPosix = os.path.join(localDirPath,
                               "./../../../lib/psutil/_psutil_posix.so")
    psutilLinuxBak = "%s_%s" % (psutilLinux, pythonVer)
    psutilPosixBak = "%s_%s" % (psutilPosix, pythonVer)

    glo_cmd = "rm -rf '%s' && cp -r '%s' '%s' " % (psutilLinux,
                                                   psutilLinuxBak,
                                                   psutilLinux)
    glo_cmd += " && rm -rf '%s' && cp -r '%s' '%s' " % (psutilPosix,
                                                        psutilPosixBak,
                                                        psutilPosix)
    psutilFlag = True
    for psutilnum in range(3):
        (status_mvPsutil, output_mvPsutil) = subprocess.getstatusoutput(
            glo_cmd)
        if (status_mvPsutil != 0):
            psutilFlag = False
            time.sleep(1)
        else:
            psutilFlag = True
            break
    if (not psutilFlag):
        print("Failed to execute cmd: %s. Error:\n%s" % (glo_cmd,
                                                         output_mvPsutil))
        sys.exit(1)
    # del error import and reload psutil
    del sys.modules['psutil._common']
    del sys.modules['psutil._psposix']
    import psutil

sys.path.append(localDirPath + "/../../")
from gspylib.common.DbClusterInfo import dbClusterInfo, \
    readOneClusterConfigItem, initParserXMLFile
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsplatform import g_Platform
from gspylib.os.gsfile import g_file
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsservice import g_service
from gspylib.hardware.gsmemory import g_memory
from gspylib.threads.parallelTool import parallelTool
from gspylib.common.VersionInfo import VersionInfo
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, \
    algorithms, modes

noPassIPs = []
g_lock = thread.allocate_lock()


def check_content_key(content, key):
    if not (type(content) == bytes):
        raise Exception(ErrorCode.GAUSS_530["GAUSS_53025"])
    elif not (type(key) in (bytes, str)):
        raise Exception(ErrorCode.GAUSS_530["GAUSS_53026"])

    iv_len = 16
    if not (len(content) >= (iv_len + 16)):
        raise Exception(ErrorCode.GAUSS_530["GAUSS_53027"])


class DefaultValue():
    """
    Default value of some variables
    """

    def __init__(self):
        pass

    TASK_INSTALL = "installation"
    TASK_UPGRADE = "upgrade"
    TASK_EXPAND = "expansion"
    TASK_REPLACE = "replacement"
    TASK_REPAIR = "repair"
    TASK_QUERY_STATUS = "status"
    TASK_START_STOP = "startup"
    TASK_START = "startup"
    TASK_STOP = "shutdown"
    TASK_SWITCH = "switching"
    TASK_GAUSSROACH = "GaussRoach"
    TASK_GAUSSROACH_SHOW = "roach_show"
    TASK_GAUSSROACH_STOP = "roach_stop_backup"
    TASK_GAUSSROACH_BACKUP = "roach_backup"
    TASK_GAUSSROACH_RESTORE = "roach_restore"
    TASK_GAUSSROACH_DELETE = "roach_delete"
    TASK_SUCCESS_FLAG = "SUCCESS"
    ###########################
    # DWS path info
    ###########################
    DWS_IMAGE_PATH = "/opt/dws/image"
    DWS_PACKAGE_PATH = "/opt/dws/package"
    DWS_APP_PAHT = "/opt/dws/app"

    # CM reload signal
    SIGNAL_RELOAD_PARA = 1
    SIGNAL_RELOAD_FILE = 9

    ###########################
    # init action timeout value
    ###########################
    # start timeout value
    TIMEOUT_CLUSTER_START = 300
    # restart nodegroup timeout value
    TIMEOUT_NODEGROUP_RESTART = 1800
    # stop timeout value
    TIMEOUT_CLUSTER_STOP = 300
    # failover timeout value
    TIMEOUT_CLUSTER_FAILOVER = 300
    # syc timeout value
    TIMEOUT_CLUSTER_SYNC = 1800
    # switch reset timeout value
    TIMEOUT_CLUSTER_SWITCHRESET = 300
    
    ##
    TIMEOUT_PSSH_COMMON = 300
    ###########################
    # pssh redis timeout value
    TIMEOUT_PSSH_REDIS = 604800
    ###########################
    # preinstall timeoutvalue
    TIMEOUT_PSSH_PREINSTALL = 1800
    # install timeout value
    TIMEOUT_PSSH_INSTALL = 1800
    # uninstall timeout value
    TIMEOUT_PSSH_UNINSTALL = 43200
    # postpreinstall timeout value
    TIMEOUT_PSSH_POSTPREINSTALL = 1800
    # binary-upgrade and rollback timeout value
    TIMEOUT_PSSH_BINARY_UPGRADE = 14400
    # expend timeout value
    TIMEOUT_PSSH_EXPEND = 43200
    # replace timeout value
    TIMEOUT_PSSH_REPLACE = 86400
    # check timeout value
    TIMEOUT_PSSH_CHECK = 1800
    # backup timeout value
    TIMEOUT_PSSH_BACKUP = 1800
    # sshexkey timeout value
    TIMEOUT_PSSH_SSHEXKEY = 1800
    # collector timeout value
    TIMEOUT_PSSH_COLLECTOR = 1800
    # start etcd timeout value
    TIMEOUT_PSSH_STARTETCD = 600
    # delCN timeout value
    TIMEOUT_PSSH_DELCN = 1800
    # addCN timeout value
    TIMEOUT_PSSH_ADDCN = 86400
    # estimate timeout value
    TIMEOUT_PSSH_ESTIMATE = 1800
    # changeip timeout value
    TIMEOUT_PSSH_CHANGEIP = 1800
    # extension connector timeout value
    TIMEOUT_PSSH_EXTENSION = 1800
    # VC mode timeout value
    TIMEOUT_PSSH_VC = 43200

    ###########################
    # init authority parameter
    ###########################
    # directory mode
    DIRECTORY_MODE = 750
    # directory permission
    DIRECTORY_PERMISSION = 0o750
    # file node
    FILE_MODE = 640
    FILE_MODE_PERMISSION = 0o640
    KEY_DIRECTORY_PERMISSION = 0o700
    KEY_FILE_MODE = 600
    MIN_FILE_MODE = 400
    MIN_FILE_PERMISSION = 0o400
    SPE_FILE_MODE = 500
    KEY_FILE_PERMISSION = 0o600
    KEY_DIRECTORY_MODE = 700
    MAX_DIRECTORY_MODE = 755
    TMP_EXE_FILE_MODE = 0o700
    SQL_FILE_MODE = 644
    # the host file permission. Do not changed it.
    HOSTS_FILE = 644
    KEY_HOSTS_FILE = 0o644

    # The available size of install app directory
    APP_DISK_SIZE = 100
    # in grey upgrade, need to install new bin instead of replacing
    # old bin in inplace upgrade
    # so need 10G to guarantee enough space
    GREY_DISK_SIZE = 10
    # The remaining space of device
    INSTANCE_DISK_SIZE = 200
    # lock cluster time
    CLUSTER_LOCK_TIME = 43200
    # lock cluster time for waiting mode
    CLUSTER_LOCK_TIME_WAIT = 3600

    # the guc paramter max_wal_senders's max value
    MAX_WAL_SENDERS = 100

    # env parameter
    MPPRC_FILE_ENV = "MPPDB_ENV_SEPARATE_PATH"
    MPPDB_TMP_PATH_ENV = "PGHOST"
    TOOL_PATH_ENV = "GPHOME"
    SUCCESS = "Success"
    FAILURE = "Failure"
    # tablespace version directory name
    # it is from gaussdb kernel code
    TABLESPACE_VERSION_DIRECTORY = "PG_9.2_201611171"
    # gauss log dir
    GAUSSDB_DIR = "/var/log/gaussdb"
    # default database name
    DEFAULT_DB_NAME = "postgres"
    # database size file
    DB_SIZE_FILE = "total_database_size"

    # current  directory path
    GURRENT_DIR_FILE = "."
    # om_monitor log directory
    OM_MONITOR_DIR_FILE = "../cm/om_monitor"
    # om_kerberos log directory
    OM_KERBEROS_DIR_FILE = "../cm/kerberos_monitor"
    # action flag file name
    ACTION_FLAG_FILE = ".action_flag_file"
    # action log file name
    DEFAULT_LOG_FILE = "gaussdb.log"
    LOCAL_LOG_FILE = "gs_local.log"
    PREINSTALL_LOG_FILE = "gs_preinstall.log"
    DEPLOY_LOG_FILE = "gs_install.log"
    REPLACE_LOG_FILE = "gs_replace.log"
    UNINSTALL_LOG_FILE = "gs_uninstall.log"
    OM_LOG_FILE = "gs_om.log"
    UPGRADE_LOG_FILE = "gs_upgradectl.log"
    CONTRACTION_LOG_FILE = "gs_shrink.log"
    DILATAION_LOG_FILE = "gs_expand.log"
    UNPREINSTALL_LOG_FILE = "gs_postuninstall.log"
    GSROACH_LOG_FILE = "gaussdb_roach.log"
    MANAGE_CN_LOG_FILE = "gs_om.log"
    GS_CHECK_LOG_FILE = "gs_check.log"
    GS_CHECKPERF_LOG_FILE = "gs_checkperf.log"
    GS_BACKUP_LOG_FILE = "gs_backup.log"
    GS_COLLECTOR_LOG_FILE = "gs_collector.log"
    GS_COLLECTOR_CONFIG_FILE = "./gspylib/etc/conf/gs_collector.json"
    GAUSS_REPLACE_LOG_FILE = "GaussReplace.log"
    GAUSS_OM_LOG_FILE = "GaussOM.log"
    TPCDS_INSTALL_LOG_FILE = "tpcd_install.log"
    LCCTL_LOG_FILE = "gs_lcctl.log"
    RESIZE_LOG_FILE = "gs_resize.log"
    HOTPATCH_LOG_FILE = "gs_hotpatch.log"
    EXPANSION_LOG_FILE = "gs_expansion.log"
    DROPNODE_LOG_FILE = "gs_dropnode.log"
    # hotpatch action
    HOTPATCH_ACTION_LIST = ["load", "unload", "active", "deactive",
                            "info", "list"]
    # cluster lock file
    CLUSTER_LOCK_PID = "gauss_cluster_lock.pid"
    # dump file for cn instance
    SCHEMA_COORDINATOR = "schema_coordinator.sql"
    # dump file for job data
    COORDINATOR_JOB_DATA = "schema_coordinator_job_data.sql"
    # dump file for statistics data
    COORDINATOR_STAT_DATA = "schema_coordinator_statistics_data.sql"
    # dump global info file for DB instance
    SCHEMA_DATANODE = "schema_datanode.sql"
    # record default group table info
    DUMP_TABLES_DATANODE = "dump_tables_datanode.dat"
    # dump default group table info file for DB instance
    DUMP_Output_DATANODE = "dump_output_datanode.sql"
    # default cluster config xml
    CLUSTER_CONFIG_PATH = "/opt/huawei/wisequery/clusterconfig.xml"
    # default alarm tools
    ALARM_COMPONENT_PATH = "/opt/huawei/snas/bin/snas_cm_cmd"
    # GPHOME
    CLUSTER_TOOL_PATH = "/opt/huawei/wisequery"
    # root scripts path
    ROOT_SCRIPTS_PATH = "/root/gauss_om"

    # package bak file name list
    PACKAGE_BACK_LIST = ["Gauss200-OLAP-Package-bak.tar.gz",
                         "Gauss200-Package-bak.tar.gz",
                         "GaussDB-Kernel-Package-bak.tar.gz"]
    # network scripts file for RHEL
    REDHAT_NETWORK_PATH = "/etc/sysconfig/network-scripts"
    # cert files list,the order of these files SHOULD NOT be modified
    CERT_FILES_LIST = ["cacert.pem",
                       "server.crt",
                       "server.key",
                       "server.key.cipher",
                       "server.key.rand",
                       "sslcrl-file.crl"]
    SSL_CRL_FILE = CERT_FILES_LIST[5]
    CERT_ROLLBACK_LIST = ["cacert.pem",
                          "server.crt",
                          "server.key",
                          "server.key.cipher",
                          "server.key.rand",
                          "sslcrl-file.crl",
                          "gsql_cert_backup.tar.gz",
                          "certFlag"]
    CLIENT_CERT_LIST = ["client.crt",
                        "client.key",
                        "client.key.cipher",
                        "client.key.rand"]
    GDS_CERT_LIST = ["cacert.pem",
                     "server.crt",
                     "server.key",
                     "server.key.cipher",
                     "server.key.rand",
                     "client.crt",
                     "client.key",
                     "client.key.cipher",
                     "client.key.rand"]
    GRPC_CERT_LIST = ["clientnew.crt",
                      "clientnew.key",
                      "cacertnew.pem",
                      "servernew.crt",
                      "servernew.key",
                      "openssl.cnf",
                      "client.key.cipher",
                      "client.key.rand",
                      "server.key.cipher",
                      "server.key.rand"]
    SERVER_CERT_LIST = ["client.crt",
                        "client.key",
                        "cacert.pem",
                        "server.crt",
                        "server.key",
                        "openssl.cnf",
                        "client.key.cipher",
                        "client.key.rand",
                        "server.key.cipher",
                        "server.key.rand",
                        "client.key.pk8"]
    BIN_CERT_LIST = ["server.key.cipher",
                     "server.key.rand"]
    CERT_BACKUP_FILE = "gsql_cert_backup.tar.gz"
    PATH_CHECK_LIST = ["|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                       "{", "}", "(", ")", "[", "]", "~", "*", "?", " ", "!",
                       "\n"]
    PASSWORD_CHECK_LIST = [";", "'", "$"]
    # The xml file path is needed by kerberos in FI_librA
    # FI_KRB_XML is used in mppdb
    FI_KRB_XML = "auth_config/mppdb-site.xml"
    # FI_ELK_KRB_XML is used in elk
    FI_ELK_KRB_XML = "auth_config/elk-krb-site.xml"
    FI_KRB_CONF = "krb5.conf"
    ###########################
    # instance role
    ###########################
    # init value
    INSTANCE_ROLE_UNDEFINED = -1
    # cm_server
    INSTANCE_ROLE_CMSERVER = 0
    # gtm
    INSTANCE_ROLE_GTM = 1
    # etcd
    INSTANCE_ROLE_ETCD = 2
    # cn
    INSTANCE_ROLE_COODINATOR = 3
    # dn
    INSTANCE_ROLE_DATANODE = 4
    # cm_agent
    INSTANCE_ROLE_CMAGENT = 5

    ###########################
    # instance type. only for CN/DN
    ###########################
    # master
    MASTER_INSTANCE = 0
    # standby
    STANDBY_INSTANCE = 1
    # dummy standby
    DUMMY_STANDBY_INSTANCE = 2
    # cascade standby
    CASCADE_STANDBY = 3

    ###########################
    # parallel number
    ###########################
    DEFAULT_PARALLEL_NUM = 12
    DEFAULT_PARALLEL_NUM_UPGRADE = 6

    # SQL_EXEC_COMMAND
    SQL_EXEC_COMMAND_WITHOUT_USER = "%s -p %s -d %s -h %s "
    SQL_EXEC_COMMAND_WITH_USER = "%s -p %s -d %s -U %s -W %s -h %s "
    SQL_EXEC_COMMAND_WITHOUT_HOST_WITHOUT_USER = "%s -p %s -d %s "
    SQL_EXEC_COMMAND_WITHOUT_HOST_WITH_USER = "%s -p %s -d %s -U %s -W %s "

    # cluster type
    CLUSTER_TYPE_SINGLE = "single"
    CLUSTER_TYPE_SINGLE_PRIMARY_MULTI_STANDBY = "single-primary-multi-standby"
    CLUSTER_TYPE_SINGLE_INST = "single-inst"

    # ssh option
    SSH_OPTION = " -o BatchMode=yes -o TCPKeepAlive=yes -o " \
                 "ServerAliveInterval=30 -o ServerAliveCountMax=10 -o " \
                 "ConnectTimeout=30 -o ConnectionAttempts=10 "
    # base64 option
    BASE_ENCODE = "encode"
    BASE_DECODE = "decode"

    # Default name of the byte stream file which contain the disabled features.
    DEFAULT_DISABLED_FEATURE_FILE_NAME = "gaussdb.version"
    # Default license control file name.
    DEFAULT_LICENSE_FILE_NAME = "gaussdb.license"

    COLLECT_CONF_JSON_KEY_LIST = [
        "Content",
        "TypeName",
        "Interval",
        "Count"
    ]
    COLLECT_CONF_CONTENT_MAP = {
        # System check config
        # cat /proc/cpuinfo;
        "HardWareInfo": "cpuInfo,memInfo,disk",
        # cat /proc/meminfo df -h
        # top; ps ux; iostat
        "RunTimeInfo": "ps,ioStat,netFlow,spaceUsage",
        # -xm 2 3; netstat; free -m du -sh
        # Log & Conf_Gstack check config
        "Coordinator": "CN",
        "DataNode": "DN",
        "Gtm": "GTM",
        # Log check config
        "ClusterManager": "cm,om,bin",
        # Core Dump check
        "gaussdb": "gaussdb",
        "gs_gtm": "gs_gtm",
        "gs_rewind": "gs_rewind",
        "cm_server": "cm_server",
        "cm_agent": "cm_agent",
        "gs_ctl": "gs_ctl",
        "gaussdb_stack": "gaussdb_stack",
        "gs_gtm_stack": "gs_gtm_stack",
        "gs_rewind_stack": "gs_rewind_stack",
        "cm_server_stack": "cm_server_stack",
        "cm_agent_stack": "cm_agent_stack",
        "gs_ctl_stack": "gs_ctl_stack",
        "AioWorker": "AioWorker",
        "AlarmChecker": "AlarmChecker",
        "Archiver": "Archiver",
        "Auditor": "Auditor",
        "AutoVacLauncher": "AutoVacLauncher",
        "AutoVacWorker": "AutoVacWorker",
        "AuxMain": "AuxMain",
        "BackendMode": "BackendMode",
        "BgWriter": "BgWriter",
        "BootStrap": "BootStrap",
        "Catchup": "Catchup",
        "CBMWriter": "CBMWriter",
        "Checkpointer": "Checkpointer",
        "CommAuxStream": "CommAuxStream",
        "CommPoolCleaner": "CommPoolCleaner",
        "CommRcvStream": "CommRcvStream",
        "CommRcvWorker": "CommRcvWorker",
        "CommSendStream": "CommSendStream",
        "CpMonitor": "CpMonitor",
        "DataRcvWriter": "DataRcvWriter",
        "DataReceiver": "DataReceiver",
        "DataSender": "DataSender",
        "ExtremeRTO": "ExtremeRTO",
        "FencedUDFMaster": "FencedUDFMaster",
        "GaussMaster": "GaussMaster",
        "Heartbeater": "Heartbeater",
        "JobExecutor": "JobExecutor",
        "LWLockMonitor": "LWLockMonitor",
        "PageWriter": "PageWriter",
        "ParallelRecov": "ParallelRecov",
        "PercentileJob": "PercentileJob",
        "Reaper": "Reaper",
        "RemoteSrv": "RemoteSrv",
        "StartupProcess": "StartupProcess",
        "StatCollector": "StatCollector",
        "Stream": "Stream",
        "SysLogger": "SysLogger",
        "ThdPoolListener": "ThdPoolListener",
        "TwoPhaseCleaner": "TwoPhaseCleaner",
        "WalRcvWriter": "WalRcvWriter",
        "WalReceiver": "WalReceiver",
        "WalSender": "WalSender",
        "WalWriter": "WalWriter",
        "WDRSnapshot": "WDRSnapshot",
        "WlmArbiter": "WlmArbiter",
        "WlmCollector": "WlmCollector",
        "WlmMonitor": "WlmMonitor"
    }

    COLLECT_CONF_MAP = {
        "System": "HardWareInfo,RunTimeInfo",
        "Database": "*",
        "Log": "Coordinator,DataNode,Gtm,ClusterManager,FFDC,AWRReport",
        "XLog": "Coordinator,DataNode",
        "Config": "Coordinator,DataNode,Gtm",
        "Gstack": "Coordinator,DataNode,Gtm",
        "CoreDump": "gaussdb,gs_gtm,gs_rewind,cm_server,cm_agent,gs_ctl,"
                    "gaussdb_stack,gs_gtm_stack,gs_rewind_stack,"
                    "cm_server_stack,cm_agent_stack,cm_server_stack,"
                    "gs_ctl_stack,AioWorker,AlarmChecker,Archiver,Auditor,"
                    "AutoVacLauncher,AutoVacWorker,AuxMain,BackendMode,"
                    "BgWriter,BootStrap,Catchup,CBMWriter,Checkpointer,"
                    "CommAuxStream,CommPoolCleaner,CommRcvStream,CommRcvWorker,"
                    "CommSendStream,CpMonitor,DataRcvWriter,DataReceiver,"
                    "DataSender,ExtremeRTO,FencedUDFMaster,GaussMaster,"
                    "Heartbeater,JobExecutor,JobScheduler,LWLockMonitor,"
                    "PageWriter,ParallelRecov,PercentileJob,Reaper,RemoteSrv,"
                    "StartupProcess,StatCollector,Stream,SysLogger,"
                    "ThdPoolListener,TwoPhaseCleaner,WalRcvWriter,WalReceiver,"
                    "WalSender,WalWriter,WDRSnapshot,WlmArbiter,WlmCollector,"
                    "WlmMonitor",
        "Trace": "Dump",
        "Plan": "*"
    }

    DATABASE_CHECK_WHITE_LIST = ["dbe_perf", "pg_catalog"]

    SYSTEM_CHECK_COMMAND_MAP = {
        "cpuInfo": "cat /proc/cpuinfo",
        "memInfo": "cat /proc/meminfo",
        "disk": "df -h",
        "ps": "ps ux",
        "ioStat": "iostat -xm 2 3",
        "netFlow": "cat /proc/net/dev",
        "spaceUsage": "free -m"
    }

    # Default retry times of SQL query attempts after successful
    # operation "gs_ctl start".
    DEFAULT_RETRY_TIMES_GS_CTL = 20
    CORE_PATH_DISK_THRESHOLD = 50

    @staticmethod
    def get_package_back_name():
        package_back_name = "%s-Package-bak_%s.tar.gz" % (
            VersionInfo.PRODUCT_NAME_PACKAGE, VersionInfo.getCommitid())
        return package_back_name

    @staticmethod
    def aes_cbc_decrypt(content, key):
        check_content_key(content, key)
        if type(key) == str:
            key = bytes(key)
        iv_len = 16
        # pre shared key iv
        iv = content[16 + 1 + 16 + 1:16 + 1 + 16 + 1 + 16]

        # pre shared key  enctryt
        enc_content = content[:iv_len]
        backend = default_backend()
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
        decrypter = cipher.decryptor()
        dec_content = decrypter.update(enc_content) + decrypter.finalize()
        dec_content = dec_content.rstrip('\0')
        server_decipher_key = dec_content[:len(dec_content) - 1]
        return server_decipher_key

    @staticmethod
    def aes_cbc_decrypt_with_path(path):
        with open(path + '/client.key.cipher', 'r') as f:
            cipher_txt = f.read()
        with open(path + '/client.key.rand', 'r') as f:
            rand_txt = f.read()

        if cipher_txt is None or cipher_txt == "":
            return None

        server_vector_cipher_vector = cipher_txt[16 + 1:16 + 1 + 16]
        # pre shared key rand
        server_key_rand = rand_txt[:16]

        # worker key
        server_decrypt_key = hashlib.pbkdf2_hmac('sha256', server_key_rand,
                                                 server_vector_cipher_vector,
                                                 10000, 16)

        enc = DefaultValue.aes_cbc_decrypt(cipher_txt, server_decrypt_key)
        return enc

    # Cert type
    GRPC_CA = "grpc"
    SERVER_CA = "server"

    @staticmethod
    def encodeParaline(cmd, keyword):
        """
        """
        if (keyword == "encode"):
            cmd = base64.b64encode(cmd.encode()).decode()
            return cmd
        if (keyword == "decode"):
            cmd = base64.b64decode(cmd.encode()).decode()
            return cmd

    @staticmethod
    def checkBondMode(bondingConfFile, isCheckOS=True):
        """
        function : Check Bond mode
        input  : String, bool
        output : List
        """
        netNameList = []

        cmd = "grep -w 'Bonding Mode' %s | awk  -F ':' '{print $NF}'" % \
              bondingConfFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 or output.strip() == ""):
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50611"] +
                            " Command:%s. Error:\n%s" % (cmd, output))

        if (isCheckOS):
            print("BondMode %s" % output.strip())

        cmd = "grep -w 'Slave Interface' %s | awk  -F ':' '{print $NF}'" % \
              bondingConfFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50611"] +
                            " Command:%s. Error:\n%s" % (cmd, output))
        for networkname in output.split('\n'):
            netNameList.append(networkname.strip())
        return netNameList

    @staticmethod
    def getNetWorkBondFlag(networkCardNum):
        """
        function: Check if the network interface card number is bondCard
                  by psutil module
        input: network interface card number
        output: FLAG, netcardList
        """
        try:
            FLAG = False
            nicAddr = ""
            netcardList = []
            netWorkInfo = psutil.net_if_addrs()
            for snic in netWorkInfo[networkCardNum]:
                if snic.family == 17:
                    nicAddr = snic.address
            if nicAddr == "":
                return FLAG, netcardList
            for net_num in netWorkInfo.keys():
                if net_num == networkCardNum:
                    continue
                for netInfo in netWorkInfo[net_num]:
                    if netInfo.address == nicAddr:
                        netcardList.append(net_num)
            if len(netcardList) >= 2:
                FLAG = True
                for net_num in netcardList:
                    cmd = "ip link | grep '%s'" % net_num
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception((ErrorCode.GAUSS_514["GAUSS_51400"] %
                                         cmd) + "\nError: %s" % output)
                    if str(output).find("master %s" % networkCardNum) == -1:
                        FLAG = False
                        netcardList = []
                        break
            return FLAG, netcardList
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] % (
                    "if the netcardNum[%s] is bondCard" % networkCardNum)
                            + " Error: \n%s" % str(e))

    @staticmethod
    def CheckNetWorkBonding(serviceIP, isCheckOS=True):
        """
        function : Check NetWork ConfFile
        input  : String, bool
        output : List
        """
        networkCardNum = DefaultValue.getNICNum(serviceIP)
        NetWorkConfFile = DefaultValue.getNetWorkConfFile(networkCardNum)
        bondingConfFile = "/proc/net/bonding/%s" % networkCardNum
        networkCardNumList = []
        networkCardNumList.append(networkCardNum)
        if os.path.exists(NetWorkConfFile):
            cmd = "grep -i 'BONDING_OPTS\|BONDING_MODULE_OPTS' %s" % \
                  NetWorkConfFile
            (status, output) = subprocess.getstatusoutput(cmd)
            if ((status == 0) and (output.strip() != "")):
                if ((output.find("mode") > 0) and os.path.exists(
                        bondingConfFile)):
                    networkCardNumList = networkCardNumList + \
                                         DefaultValue.checkBondMode(
                                             bondingConfFile, isCheckOS)
                else:
                    raise Exception(ErrorCode.GAUSS_506["GAUSS_50611"] +
                                    " Command:%s. Error:\n%s" % (cmd, output))
            elif isCheckOS:
                print("BondMode Null")
        else:
            (flag, netcardList) = DefaultValue.getNetWorkBondFlag(
                networkCardNum)
            if flag:
                if os.path.exists(bondingConfFile):
                    networkCardNumList = networkCardNumList + \
                                         DefaultValue.checkBondMode(
                                             bondingConfFile, isCheckOS)
                else:
                    sys.exit(ErrorCode.GAUSS_506["GAUSS_50611"] +
                             "Without NetWorkConfFile mode.")
            else:
                print("BondMode Null")
        if (len(networkCardNumList) != 1):
            del networkCardNumList[0]
        return networkCardNumList

    @staticmethod
    def checkNetWorkMTU(nodeIp, isCheckOS=True):
        """
        function: gs_check check NetWork card MTU parameters
        input: string, string
        output: int
        """
        try:
            networkCardNum = DefaultValue.CheckNetWorkBonding(nodeIp,
                                                              isCheckOS)
            mtuValue = psutil.net_if_stats()[networkCardNum[0]].mtu
            if (not mtuValue):
                return "        Abnormal reason: Failed to obtain " \
                       "network card MTU value."
            return mtuValue
        except Exception as e:
            return "        Abnormal reason: Failed to obtain the " \
                   "networkCard parameter [MTU]. Error: \n        %s" % str(e)

    @staticmethod
    def getNetWorkConfFile(networkCardNum):
        """
        function : Get NetWork ConfFile
        input  : int
        output : String
        """
        SuSENetWorkConfPath = "/etc/sysconfig/network"
        RedHatNetWorkConfPath = "/etc/sysconfig/network-scripts"
        NetWorkConfFile = ""
        distname, version, idnum = g_Platform.dist()
        distname = distname.lower()
        if (distname in ("redhat", "centos", "euleros", "openEuler")):
            NetWorkConfFile = "%s/ifcfg-%s" % (RedHatNetWorkConfPath,
                                               networkCardNum)
        else:
            NetWorkConfFile = "%s/ifcfg-%s" % (SuSENetWorkConfPath,
                                               networkCardNum)

        if (not os.path.exists(NetWorkConfFile)):
            if (distname in (
                    "redhat", "centos", "euleros", "openeuler")):
                cmd = "find %s -iname 'ifcfg-*-%s' -print" % (
                    RedHatNetWorkConfPath, networkCardNum)
            else:
                cmd = "find %s -iname 'ifcfg-*-%s' -print" % (
                    SuSENetWorkConfPath, networkCardNum)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 and DefaultValue.checkDockerEnv()):
                return output.strip()
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd)
            if (len(output.split('\n')) != 1):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                NetWorkConfFile)

            NetWorkConfFile = output.strip()

        return NetWorkConfFile

    @staticmethod
    def getNICNum(ipAddress):
        """
        function: Obtain network interface card number by psutil module
        input: ipAddress
        output: netWorkNum
        """
        try:
            netWorkNum = ""
            netWorkInfo = psutil.net_if_addrs()
            for nic_num in netWorkInfo.keys():
                for netInfo in netWorkInfo[nic_num]:
                    if netInfo.address == ipAddress:
                        netWorkNum = nic_num
                        break
            if netWorkNum == "":
                raise Exception(ErrorCode.GAUSS_506["GAUSS_50604"] % ipAddress)
            return netWorkNum
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50604"] % ipAddress +
                            " Error: \n%s" % str(e))

    @staticmethod
    def getIpAddressList():
        """
        """
        # Obtain all Ips by psutil module
        try:
            ipAddressList = []
            netWorkInfo = psutil.net_if_addrs()
            for per_num in netWorkInfo.keys():
                netInfo = netWorkInfo[per_num][0]
                if (len(netInfo.address.split('.')) == 4):
                    ipAddressList.append(netInfo.address)
            if (len(ipAddressList) == 0):
                raise Exception(ErrorCode.GAUSS_506["GAUSS_50616"])
            return ipAddressList
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50616"] +
                            " Error: \n%s" % str(e))

    @staticmethod
    def getIpByHostName():
        '''
        function: get local host ip by the hostname
        input : NA
        output: hostIp
        '''
        # get hostname
        hostname = socket.gethostname()

        # get local host in /etc/hosts
        cmd = "grep -E \"^[1-9 \\t].*%s[ \\t]*#Gauss.* IP Hosts Mapping$\" " \
              "/etc/hosts | grep -E \" %s \"" % (hostname, hostname)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0 and output != ""):
            hostIp = output.strip().split(' ')[0].strip()
            return hostIp

        # get local host by os function
        hostIp = socket.gethostbyname(hostname)
        return hostIp

    @staticmethod
    def GetHostIpOrName():
        """
        function: Obtaining the local IP address
        input: NA
        output: NA
        """
        return g_OSlib.getHostName()

    @staticmethod
    def GetPythonUCS():
        """
        function: get python3 unicode value. Using it to chose which
                  Crypto we need.
                  1114111 is Crypto_UCS4
                  65535 is Crypto_UCS2
                  the value 0 is only grammar support.
        input: NA
        output: NA
        """
        if sys.maxunicode == 1114111:
            return 4
        elif sys.maxunicode == 65535:
            return 2
        else:
            return 0

    @staticmethod
    def checkPythonVersion():
        """
        function : Check system comes with Python version
        input : NA
        output: list
        """
        (major, minor, patchlevel) = platform.python_version_tuple()
        if (str(major) == '3' and (str(minor) in ['6', '7'])):
            if (str(minor) == '6'):
                return (True, "3.6")
            else:
                return (True, "3.7")
        else:
            return (False, "%s.%s.%s" % (str(major), str(minor),
                                         str(patchlevel)))

    @staticmethod
    def getUserId(user):
        """
        function : get user id
        input : user
        output : user id
        """
        try:
            pwd.getpwnam(user).pw_uid
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50300"] % user +
                            "Detail msg: %s" % str(e))

    @staticmethod
    def checkUser(user, strict=True):
        """
        function : Check if user exists and if is the right user
        input : String,boolean
        output : NA
        """
        # get group
        try:
            DefaultValue.getUserId(user)
        except Exception as e:
            raise Exception(str(e))

        # if not strict, skip
        if (not strict):
            return

        # get $GAUSS_ENV, and makesure the result is correct.
        mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
        if (mpprcFile != "" and mpprcFile is not None):
            gaussEnv = DefaultValue.getEnvironmentParameterValue("GAUSS_ENV",
                                                                 user,
                                                                 mpprcFile)
        else:
            gaussEnv = DefaultValue.getEnvironmentParameterValue("GAUSS_ENV",
                                                                 user,
                                                                 "~/.bashrc")
        if not gaussEnv or str(gaussEnv) != "2":
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50300"] %
                            ("installation path of designated user %s" % user)
                            + " Maybe the user is not right.")

    @staticmethod
    def getMpprcFile():
        """
        function : get mpprc file
        input : NA
        output : String
        """
        try:
            # get mpp file by env parameter MPPDB_ENV_SEPARATE_PATH
            mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
            if (mpprcFile != "" and mpprcFile is not None):
                userProfile = mpprcFile
                if (not os.path.isabs(userProfile)):
                    raise Exception(ErrorCode.GAUSS_512["GAUSS_51206"] %
                                    userProfile)
                if (not os.path.exists(userProfile)):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                    userProfile)
            elif (os.getuid() == 0):
                return "/etc/profile"
            else:
                userAbsoluteHomePath = g_Platform.getUserHomePath()
                userProfile = os.path.join(userAbsoluteHomePath, ".bashrc")
            if (not os.path.isfile(userProfile)):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] %
                                userProfile)
            return userProfile
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def isIpValid(ip):
        """
        function : check if the input ip address is valid
        input : String
        output : NA
        """
        Valid = re.match("^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]"
                         "{1}|[1-9])\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|"
                         "[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|"
                         "[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\."
                         "(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}"
                         "[0-9]{1}|[0-9])$", ip)
        if Valid:
            if (Valid.group() == ip):
                return True
        return False

    @staticmethod
    def doConfigForParamiko():
        """
        function: Config depend file for pramiko 2.4.2. wen only support 2.7.x
        input : NA
        output: NA
        """
        (result, version) = DefaultValue.checkPythonVersion()
        if not result:
            print(ErrorCode.GAUSS_522["GAUSS_52201"] % version +
                  " It must be 3.6.x or 3.7.x.")
            sys.exit(1)
        else:
            localDir = os.path.dirname(os.path.realpath(__file__))
            omToolsCffiPath = os.path.join(localDir,
                                           "./../../../lib/_cffi_backend.so")
            inspectToolsCffiPath = os.path.join(
                localDir, "./../../../script/gspylib/inspection/"
                          "lib/_cffi_backend.so")

            """
            Never remove _cffi_backend.so_UCS4 folder, as there maybe 
            multi-version pythons on the platform
            (V1R8C10 is with its own python, but now, we don't package 
            python any more).
            """
            try:
                flagNum = int(DefaultValue.GetPythonUCS())
                # clean the old path info
                g_file.removeFile(omToolsCffiPath)
                g_file.removeFile(inspectToolsCffiPath)
                # copy the correct version
                newPythonDependCryptoPath = "%s_UCS%d_%s" % (omToolsCffiPath,
                                                          flagNum,version)
                if os.path.exists(newPythonDependCryptoPath):
                    g_file.cpFile(newPythonDependCryptoPath, omToolsCffiPath,
                                  "shell")
                    g_file.cpFile(newPythonDependCryptoPath, inspectToolsCffiPath,
                                  "shell")
                else:
                    newPythonDependCryptoPath = "%s_UCS%d" % (omToolsCffiPath,
                                                          flagNum)
                    g_file.cpFile(newPythonDependCryptoPath, omToolsCffiPath,
                                  "shell")
                    g_file.cpFile(newPythonDependCryptoPath, inspectToolsCffiPath,
                                  "shell")
            except Exception as e:
                print(ErrorCode.GAUSS_516["GAUSS_51632"] %
                      ("config depend file for paramiko 2.6.0. "
                       "Error:\n%s" % str(e)))
                sys.exit(1)
            sys.path.insert(0, os.path.join(localDir, "./../../lib"))

    @staticmethod
    def getInstallDir(user):
        """
        function : Get the installation directory for user
        input : NA
        output : String
        """
        # get the installation directory for user by $GAUSSHOME
        gaussHome = DefaultValue.getEnvironmentParameterValue("GAUSSHOME",
                                                              user)
        return gaussHome

    @staticmethod
    def getTmpDir(user, xml_path):
        """
        function : Get the temporary directory for user
        input : NA
        output : String 
        """
        return dbClusterInfo.readClusterTmpMppdbPath(user, xml_path)

    @staticmethod
    def getTmpDirFromEnv(user=""):
        """
        function : Get the temporary directory from PGHOST
        precondition: only root user or install user can call this function
        input : String
        output : String
        """
        tmpDir = ""
        if (os.getuid() == 0 and user == ""):
            return tmpDir
        # get the temporary directory from PGHOST
        tmpDir = DefaultValue.getEnvironmentParameterValue("PGHOST", user)
        return tmpDir

    @staticmethod
    def getTmpFileFromEnv(fileName="", user="", desc=""):
        """
        function : Get the temporary directory from PGHOST
        precondition: only root user or install user can call this function
        input : String
        output : String
        """
        tmpDir = DefaultValue.getTmpDirFromEnv(user)

        # get current time
        currentTime = time.strftime("%Y-%m-%d_%H%M%S")
        # split the log file by '.'
        # rebuild the file name
        # before rebuild:        prefix.suffix
        # after  rebuild:        prefix-currentTime-pid-desc.suffix
        if fileName.find(".") >= 0:
            tmpList = fileName.split(".")
            prefix = tmpList[0]
            suffix = tmpList[1]
            if (desc == ""):
                tmpFile = os.path.join(tmpDir, "%s-%s-%d.%s" % (
                    prefix, currentTime, os.getpid(), suffix))
            else:
                tmpFile = os.path.join(tmpDir, "%s-%s-%d-%s.%s" % (
                    prefix, currentTime, os.getpid(), desc, suffix))
        else:
            tmpFile = os.path.join(tmpDir, "%s-%s-%d" % (fileName, currentTime,
                                                         os.getpid()))
        return tmpFile

    @staticmethod
    def getTmpDirAppendMppdb(user):
        """
        function : Get the user's temporary directory 
        input : String
        output : String
        """
        # get the user's temporary directory 
        tmpDir = DefaultValue.getTmpDirFromEnv(user)
        # if the env paramter not exist, return ""
        if (tmpDir == ""):
            return tmpDir
        # modify tmp dir
        forbidenTmpDir = "/tmp/%s" % user
        if (tmpDir == forbidenTmpDir):
            tmpDir = os.path.join(DefaultValue.getEnv("GPHOME"),
                                  "%s_mppdb" % user)
        return tmpDir

    @staticmethod
    def getUserFromXml(xml_path):
        """
        function : Get the user from xml file
        input : String
        output : String
        """
        # the function must return a value. no matter it is correct or not
        try:
            bin_path = dbClusterInfo.readClusterAppPath(xml_path)
            DefaultValue.checkPathVaild(bin_path)
            user = g_OSlib.getPathOwner(bin_path)[0]
        except Exception as e:
            user = ""

        return user

    @staticmethod
    def getEnvironmentParameterValue(environmentParameterName, user,
                                     env_file=None):
        """
        function : Get the environment parameter value from user
        input : String,String
        output : String
        """
        userFlag = False
        cmd = "cat /etc/passwd|grep -v nologin|grep -v halt|grep -v " \
              "shutdown|" \
              "awk -F: '{ print $1 }'| grep '^%s$' 2>/dev/null" % user
        status, output = subprocess.getstatusoutput(cmd)
        if output and status == 0:
            DefaultValue.getUserId(user)
            # User exists, need to check passwd.
            userFlag = True

        if userFlag and os.getuid() == 0:
            # Only user with root permission need check if password must
            # change.
            DefaultValue.checkPasswdForceChange(user)

        if (env_file is not None):
            userProfile = env_file
        else:
            userProfile = DefaultValue.getMpprcFile()
        # buid the shell command
        executeCmd = "echo $%s" % environmentParameterName
        cmd = g_Platform.getExecuteCmdWithUserProfile(user, userProfile,
                                                      executeCmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0):
            EnvValue = output.split("\n")[0]
            EnvValue = EnvValue.replace("\\", "\\\\").replace('"', '\\"\\"')
            DefaultValue.checkPathVaild(EnvValue)
            return EnvValue
        else:
            return ""

    @staticmethod
    def checkPasswdForceChange(checkUser):
        """
        function: Check if user password is forced to change at next login.
        input : user name
        output: NA
        """
        distname, version, currentid = g_Platform.dist()
        if (distname.lower() in ("suse", "redhat", "centos", "euleros",
                                 "openeuler")):
            cmd = g_file.SHELL_CMD_DICT["checkPassword"] % (checkUser,
                                                            "'^Last.*Change'")
        else:
            return
        (timestatus, output) = subprocess.getstatusoutput(cmd)
        if (timestatus != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)
        if (output == ""):
            return
        result = output.split(":")[1].strip()
        # If passwd is forced to change. Throw error code.
        if (distname.lower() == "suse"):
            if (version == '11'):
                if ("password is forced to change at next login" in result):
                    raise Exception(ErrorCode.GAUSS_503["GAUSS_50307"])
            elif (version == '12'):
                if ("password must be changed" in result):
                    raise Exception(ErrorCode.GAUSS_503["GAUSS_50307"])
        if (distname.lower() in ("redhat", "centos", "euleros",
                                 "openeuler")):
            if ("password must be changed" in result):
                raise Exception(ErrorCode.GAUSS_503["GAUSS_50307"])

    @staticmethod
    def getClusterToolPath():
        """
        function : Get the value of cluster's tool path.
                   The value can't be None or null
        input : NA
        output : String
        """
        mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
        echoEnvCmd = "echo $%s" % DefaultValue.TOOL_PATH_ENV
        if not mpprcFile:
            mpprcFile = "/etc/profile"
        cmd = g_Platform.getExecuteCmdWithUserProfile("", mpprcFile,
                                                      echoEnvCmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] %
                            DefaultValue.TOOL_PATH_ENV +
                            " Command:%s. Error:\n%s" % (cmd, output))

        clusterToolPath = output.split("\n")[0]
        if not clusterToolPath:
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] %
                            DefaultValue.TOOL_PATH_ENV + "Value: %s." %
                            clusterToolPath)

        # Check if the path contains illegal characters
        DefaultValue.checkPathVaild(clusterToolPath)

        return clusterToolPath

    @staticmethod
    def getPreClusterToolPath(user, xml):
        """
        function: get the cluster tool path
        input : NA
        output: NA
        """
        try:
            configedPath = DefaultValue.getOneClusterConfigItem(
                "gaussdbToolPath", user, xml)
            if (configedPath == ""):
                configedPath = DefaultValue.CLUSTER_TOOL_PATH
            DefaultValue.checkPathVaild(configedPath)
            return configedPath
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def getOneClusterConfigItem(item_name, user, xml):
        """
        function: get the OM log path
        input : NA
        output: NA
        """
        try:
            # set env paramter CLUSTERCONFIGFILE
            os.putenv("CLUSTERCONFIGFILE", xml)
            # read one cluster configuration item "cluster"
            (retStatus, retValue) = readOneClusterConfigItem(
                initParserXMLFile(xml), item_name, "cluster")
            if (retStatus == 0):
                return os.path.normpath(retValue)
            else:
                return ""
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def getUserLogDirWithUser(user):
        """
        function : Get the log directory from user
        input : String
        output : String
        """
        log_path = ""
        try:
            log_path = DefaultValue.getEnvironmentParameterValue("GAUSSLOG",
                                                                 user)
        except Exception as e:
            log_path = "%s/%s" % (DefaultValue.GAUSSDB_DIR, user)
        return log_path

    @staticmethod
    def getOMLogPath(logName, user="", appPath="", xml="", action=""):
        """
        function : Get the OM log path from xml file
        input : String
        output : String
        """
        logPath = ""
        try:
            if (user != "" and xml != ""):
                logPath = "%s" % dbClusterInfo.readClusterLogPath(xml)
                path = "%s/%s/om/%s" % (logPath, user, logName)
            elif (action == "virtualip"):
                path = "/var/log/gs_virtualip/%s" % (logName)
            elif (user != ""):
                logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)
            elif (appPath != ""):
                user = g_OSlib.getPathOwner(appPath)[0]
                if (user == ""):
                    user = "."
                if (user == "."):
                    logPath = DefaultValue.GAUSSDB_DIR
                else:
                    logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)
            elif (xml != ""):
                try:
                    appPath = dbClusterInfo.readClusterAppPath(xml)
                    user = g_OSlib.getPathOwner(appPath)[0]
                except Exception as e:
                    user = "."
                if (user == ""):
                    user = "."
                if (user == "."):
                    logPath = DefaultValue.GAUSSDB_DIR
                else:
                    logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)
            else:
                logPath = DefaultValue.GAUSSDB_DIR
                path = "%s/om/%s" % (logPath, logName)
        except Exception as e:
            logPath = DefaultValue.GAUSSDB_DIR
            path = "%s/om/%s" % (logPath, DefaultValue.LOCAL_LOG_FILE)

        return os.path.realpath(path)

    @staticmethod
    def getBackupDir(subDir=""):
        """
        function : Get the cluster's default backup directory for upgrade
        input : String
        output : String
        """
        bakDir = "%s/backup" % DefaultValue.getClusterToolPath()
        if (subDir != ""):
            bakDir = os.path.join(bakDir, subDir)

        return bakDir

    @staticmethod
    def getAppVersion(appPath=""):
        """
        function : Get the version of application by $GAUSS_VERSION
        input : String
        output : String
        """
        # get user and group
        (user, group) = g_OSlib.getPathOwner(appPath)
        if (user == "" or group == ""):
            return ""

        # build shell command
        # get the version of application by $GAUSS_VERSION
        gaussVersion = DefaultValue.getEnvironmentParameterValue(
            "GAUSS_VERSION", user)
        return gaussVersion

    @staticmethod
    def getUserHome(user=""):
        """
        function :Get the user Home
        input : String
        output : String
        """
        cmd = "su - %s -c \"echo ~\" 2>/dev/null" % user
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0 or output.strip() == "":
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error:\n%s" % output)
        return output.strip()


    @staticmethod
    def getAppBVersion(appPath=""):
        """
        function :Get the version of application by $GAUSS_VERSION
        input : String
        output : String 
        """
        # get user and group
        (user, group) = g_OSlib.getPathOwner(appPath)
        if (user == "" or group == ""):
            return ""
        # build shell command
        userProfile = DefaultValue.getMpprcFile()
        executeCmd = "gaussdb -V"
        cmd = g_Platform.getExecuteCmdWithUserProfile(user, userProfile,
                                                      executeCmd, False)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            return ""
        return output.replace('gaussdb ', '').strip()

    @staticmethod
    def getOSInitFile():
        """
        function : Get the OS initialization file
        input : NA
        output : String
        """
        distname, version, currentid = g_Platform.dist()
        systemDir = "/usr/lib/systemd/system/"
        systemFile = "/usr/lib/systemd/system/gs-OS-set.service"
        # OS init file 
        #     now we only support SuSE and RHEL/CentOS
        initFileSuse = "/etc/init.d/boot.local"
        initFileRedhat = "/etc/rc.d/rc.local"
        # system init file
        initSystemFile = "/usr/local/gauss/script/gauss-OS-set.sh"
        initSystemPath = "/usr/local/gauss/script"
        dirName = os.path.dirname(os.path.realpath(__file__))

        # Get the startup file of suse or redhat os
        if (os.path.isdir(systemDir)):
            # Judge if cgroup para 'Delegate=yes' is written in systemFile
            cgroup_gate = False
            cgroup_gate_para = "Delegate=yes"
            if os.path.exists(systemFile):
                with open(systemFile, 'r') as fp:
                    retValue = fp.readlines()
                for line in retValue:
                    if line.strip() == cgroup_gate_para:
                        cgroup_gate = True
                        break

            if (not os.path.exists(systemFile) or not cgroup_gate):
                srcFile = "%s/../etc/conf/gs-OS-set.service" % dirName
                g_file.cpFile(srcFile, systemFile)
                g_file.changeMode(DefaultValue.KEY_FILE_MODE, systemFile)
                # only support RHEL/Centos/Euler
                if (distname != "SuSE"):
                    # enable gs-OS-set.service
                    (status, output) = g_service.manageOSService("gs-OS-set",
                                                                 "enable")
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_508["GAUSS_50802"] %
                                        "enable gs-OS-set" + " Error: \n%s" %
                                        output)

            if (not os.path.exists(initSystemPath)):
                g_file.createDirectory(initSystemPath)
            if (not os.path.exists(initSystemFile)):
                g_file.createFile(initSystemFile, False)
                g_file.writeFile(initSystemFile, ["#!/bin/bash"], "w")
            g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, initSystemFile)
            return initSystemFile
        if (distname == "SuSE" and os.path.isfile(initFileSuse)):
            initFile = initFileSuse
        elif (distname in ("redhat", "centos", "euleros", "openEuler") and
              os.path.isfile(initFileRedhat)):
            initFile = initFileRedhat
        else:
            initFile = ""

        return initFile

    @staticmethod
    def getNetworkConfiguredFile(ip):
        """
        function: get network configuration file
        input: ip
        output: networkFile
        """
        pattern = re.compile("ifcfg-.*:.*")
        networkFile = ""
        try:
            for filename in os.listdir(DefaultValue.REDHAT_NETWORK_PATH):
                result = pattern.match(filename)
                if (result is None):
                    continue
                paramfile = "%s/%s" % (DefaultValue.REDHAT_NETWORK_PATH,
                                       filename)
                with open(paramfile, "r") as fp:
                    fileInfo = fp.readlines()
                # The current opened file is generated while configing
                # virtual IP,
                # there are 3 lines in file, and the second line is IPADDR=IP
                if len(fileInfo) == 3 and \
                        fileInfo[1].find("IPADDR=%s" % ip) >= 0:
                    networkFile += "%s " % paramfile
            return networkFile
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "network configuration file" +
                            " Error: \n%s " % str(e))

    @staticmethod
    def getMatchingResult(matchExpression, fileMatching, remoteHostName=""):
        """
        """
        cmd = "%s -E '%s' %s" % (g_Platform.getGrepCmd(),
                                 matchExpression, fileMatching)
        if ("" != remoteHostName and remoteHostName !=
                DefaultValue.GetHostIpOrName()):
            cmd = g_OSlib.getSshCommand(remoteHostName, cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        return (status, output)

    @staticmethod
    def preConfigFile(filename):
        """
        function: pretreatment configuration file, delete the ' ' or
                  '\t' when they top of line
        input: filename
        output: NA
        """
        try:
            (status, output) = DefaultValue.getMatchingResult("^[ \\t]",
                                                              filename)
            if (status != 0):
                return
            listLine = output.split('\n')
            for strline in listLine:
                g_file.replaceFileLineContent("^%s$" % strline,
                                              strline.strip(), filename)

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def getConfigFilePara(configFile, section, checkList=None,
                          optionsName=None):
        """
        function: get the configuration file(check_list.conf)
        input: section: the section in check_list.conf will be get
               optionsName: the parameter list will be get, if parameter
               is NULL, then get all
        output: dist
        """
        if checkList is None:
            checkList = []
        if optionsName is None:
            optionsName = []
        try:
            DefaultValue.preConfigFile(configFile)

            # read the check_list.conf
            data = {}
            fp = configparser.RawConfigParser()
            fp.read(configFile)

            # get the sections then check the section whether or not
            # in check_list.conf
            secs = fp.sections()
            if section not in secs:
                return data

            # get the parameters then check options whether or not in
            # section parameters
            optionList = fp.options(section)
            if (len(optionsName) != 0 and optionsName not in optionList):
                return data
            elif (len(optionsName) != 0):
                optionList = optionsName

            # get th parameter values
            for key in optionList:
                value = fp.get(section, key)
                if (len(value.split()) == 0):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50012"] % key)
                value = value.split('#')[0]
                if (key in checkList and not value.isdigit()):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50003"]
                                    % (key, "digit"))
                if (section == '/etc/security/limits.conf' and not
                value.isdigit() and value != 'unlimited'):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"] % key)
                data[key] = value

            if ("vm.min_free_kbytes" in list(data.keys())):
                swapTotalSize = g_memory.getMemTotalSize() // 1024
                multiple = data["vm.min_free_kbytes"].split('*')[1].split('%')[
                    0].strip()
                val = int(swapTotalSize) * int(multiple) // 100
                data["vm.min_free_kbytes"] = str(val)

            return data
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51234"] % configFile +
                            " Error: \n%s" % str(e))

    @staticmethod
    def checkInList(listsrc, listdest):
        """
        function: check the listsrc element is not in listdest
        input: listsrc, listdest
        output: True or False
        """
        if (listsrc == [] or listdest == []):
            return False

        for key in listsrc:
            if (key in listdest):
                return True
        return False

    @staticmethod
    def checkSSDInstalled():
        """
        function: check SSD 
        input: NA
        output: True/False
        """
        cmd = "hio_info"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            return False
        return True

    @staticmethod
    def Deduplication(listname):
        """
        function: Deduplication the list
        input : NA
        output: NA
        """
        listname.sort()
        for i in range(len(listname) - 2, -1, -1):
            if listname.count(listname[i]) > 1:
                del listname[i]
        return listname

    @staticmethod
    def getEnv(envparam, default_value=None):
        """
        function: get the filter environment variable
        input:envparam: String
              default_value: String
        output:envValue
        """
        try:
            envValue = os.getenv(envparam)

            if envValue is None:
                if default_value:
                    return default_value
                else:
                    return envValue

            envValue = envValue.replace("\\", "\\\\").replace('"', '\\"\\"')

            DefaultValue.checkPathVaild(envValue)

            return envValue
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkPathVaild(envValue):
        """
        function: check path vaild
        input : envValue
        output: NA
        """
        if (envValue.strip() == ""):
            return
        for rac in DefaultValue.PATH_CHECK_LIST:
            flag = envValue.find(rac)
            if flag >= 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % envValue +
                                " There are illegal characters in the path.")

    @staticmethod
    def checkPasswordVaild(password, user="", clusterInfo=None):
        """
        function: check password vaild
        input : password
        output: NA
        """
        # rule1: check if the password contains illegal characters
        for rac in DefaultValue.PASSWORD_CHECK_LIST:
            flag = password.find(rac)
            if flag >= 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                                "the password" + " The password contains "
                                                 "illegal characters.")

    @staticmethod
    def getPathFileOfENV(envName):
        """
        function : Get the env.
        input : envName
        output       
        """
        value = DefaultValue.getEnv(envName)
        if (value and not g_file.checkClusterPath(value)):
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51805"] % envName +
                            "It may have been modified after the cluster "
                            "installation is complete.")
        return value

    @staticmethod
    def checkPackageOS():
        """
        function : get and check binary file
        input : NA
        output : boolean
        """
        try:
            (fileSHA256, sha256Value) = g_OSlib.getFileSHA256Info()
            if (fileSHA256 != sha256Value):
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51635"] +
                                "The SHA256 value is different. \nBin file: "
                                "%s\nSHA256 file: %s." % (fileSHA256,
                                                          sha256Value))
            return True
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def removeTmpMpp(mpprcFile):
        mppTmp_rm = os.path.dirname(mpprcFile) + "/mpprcfile_tmp"
        if (os.path.exists(mppTmp_rm)):
            g_file.removeDirectory(mppTmp_rm)

    @staticmethod
    def checkRemoteDir(g_sshTool, remoteDir, hostname, mpprcFile="",
                       localMode=False):
        '''
        function: check the remoteDir is existing on hostname
        input: remoteDir, hostname, mpprcFile
        output:NA
        '''
        try:
            # check package dir
            # package path permission can not change to 750, or it will have
            # permission issue.
            toolpath = remoteDir.split("/")
            toolpath[0] = "/" + toolpath[0]
            pathcmd = ""
            for path in toolpath:
                if (path == ""):
                    continue
                cmd = g_file.SHELL_CMD_DICT["createDir"] % \
                      (path, path, DefaultValue.MAX_DIRECTORY_MODE)
                pathcmd += "%s; cd '%s';" % (cmd, path)
            pathcmd = pathcmd[:-1]
            DefaultValue.execCommandWithMode(pathcmd,
                                             "check package directory",
                                             g_sshTool,
                                             localMode,
                                             mpprcFile,
                                             hostname)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkAllNodesMpprcFile(hostList, appPath, mpprcFile):
        """
        function:check All Nodes MpprcFile
        input: hostList, appPath, mpprcFile
        output:NA
        """
        # get mppfile, make sure it exists
        if mpprcFile is None or mpprcFile == "/etc/profile" or mpprcFile == \
                "~/.bashrc" or \
                not os.path.exists(mpprcFile):
            return
        if (len(hostList) == 0):
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51203"] % "hostanme")
        mppTmp = os.path.dirname(mpprcFile) + "/mpprcfile_tmp"
        # Clean old tmp dir
        DefaultValue.removeTmpMpp(mpprcFile)
        # Create tmp dir for all mppfile
        g_file.createDirectory(mppTmp)
        # Copy every mppfile, rename them by hostname
        for host in hostList:
            catCmd = "%s %s > /dev/null 2>&1" % (g_Platform.getCatCmd(),
                                                 mpprcFile)
            cmd = g_OSlib.getSshCommand(host, catCmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                tmpEnv = "%s/%s_env" % (mppTmp, host)
                scpCmd = g_Platform.getRemoteCopyCmd(mpprcFile, tmpEnv, host,
                                                     False)
                (status, output) = subprocess.getstatusoutput(scpCmd)
                DefaultValue.execCommandLocally(scpCmd)
                DefaultValue.checkMpprcFileChange(tmpEnv, host, mpprcFile)

        # remove tmp dir
        DefaultValue.removeTmpMpp(mpprcFile)

    @staticmethod
    def checkMpprcFileChange(mpprcFile, host="local host", mpprcFile_rm=""):
        """
        function:Check if mppfile has been changed
        input: mppfile
        output:NA
        """
        # get mppfile, make sure it exists
        if mpprcFile == "" or mpprcFile is None or mpprcFile == \
                "/etc/profile" or mpprcFile == "~/.bashrc" or \
                not os.path.exists(mpprcFile):
            DefaultValue.removeTmpMpp(mpprcFile)
            return

        if host == "" or host is None:
            host = "local host"

        # read the content of mppfile
        with open(mpprcFile, 'r') as fp:
            mpp_content = fp.read()
            env_list = mpp_content.split('\n')
        while '' in env_list:
            env_list.remove('')
        # remove ec content from list
        for env in env_list:
            if re.match("^if \[ -f .*\/env_ec", env):
                env_list.remove(env)
                break

        # white elements
        list_white = ["ELK_CONFIG_DIR", "ELK_SYSTEM_TABLESPACE",
                      "MPPDB_ENV_SEPARATE_PATH", "GPHOME", "PATH",
                      "LD_LIBRARY_PATH", "PYTHONPATH", "GAUSS_WARNING_TYPE",
                      "GAUSSHOME", "PATH", "LD_LIBRARY_PATH",
                      "S3_CLIENT_CRT_FILE", "GAUSS_VERSION", "PGHOST",
                      "GS_CLUSTER_NAME", "GAUSSLOG", "GAUSS_ENV", "umask"]
        # black elements
        list_black = ["|", ";", "&", "<", ">", "`", "\\", "!", "\n"]

        # check mpprcfile
        for env in env_list:
            env = env.strip()
            if (env == ""):
                continue
            for white in list_white:
                flag_white = 0
                flag = env.find(white)
                if (env.startswith('export') or flag >= 0):
                    flag_white = 1
                    break
            if (flag_white == 0):
                DefaultValue.removeTmpMpp(mpprcFile_rm)
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % env +
                                " There are illegal characters in %s." % host)
            for black in list_black:
                flag = env.find(black)
                if (flag >= 0 and env != ""):
                    DefaultValue.removeTmpMpp(mpprcFile_rm)
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % env +
                                    " There are illegal characters in %s." %
                                    host)

    @staticmethod
    def sourceEnvFile(file_env):
        """
        """
        cmd = "%s '%s'" % (g_Platform.getSourceCmd(), file_env)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 or output.strip() != ""):
            return (False, output)
        return (True, "")

    @staticmethod
    def checkEnvFile(mpprcFile="", user=""):
        """
        function: check if the env file contains msg which may cause the
        program failed.
        input: NA
        output: NA
        """
        (status, output) = DefaultValue.sourceEnvFile("/etc/profile")
        if (status != True):
            return (False, output)

        if (mpprcFile != "" and os.path.isfile(mpprcFile)):
            (status, output) = DefaultValue.sourceEnvFile(mpprcFile)
            if (status != True):
                return (False, output)

        if ((user != "") and (os.getuid() == 0)):
            executeCmd = "%s '%s' && %s '%s'" % (g_Platform.getSourceCmd(),
                                                 "/etc/profile",
                                                 g_Platform.getSourceCmd(),
                                                 "~/.bashrc")
            if (mpprcFile != ""):
                remoteSourceCmd = "if [ -f '%s' ] ; then %s '%s'; fi" % \
                                  (mpprcFile, g_Platform.getSourceCmd(),
                                   mpprcFile)
                executeCmd = "%s && %s" % (executeCmd, remoteSourceCmd)
            cmd = g_Platform.getExecuteCmdWithUserProfile(user, "~/.bashrc",
                                                          executeCmd, False)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 or output.strip() != ""):
                return (False, output)
        return (True, "")

    @staticmethod
    def createPathUnderRoot(newPath, permission, user="", group=""):
        """
        function: 1.create path using root user  2.modify the path permission
        notice: this function only can be called by root, and user and group
        should be exist
        input : newPath: the path we want to create.
                   permission: the permission of the path.
                   user: the user of the created path.
                   group: the group of the input user.
        output: NA
        """
        # check if exist and create new path
        ownerPath = newPath
        if (not os.path.exists(ownerPath)):
            ownerPath = DefaultValue.getTopPathNotExist(ownerPath)

        if (not os.path.isdir(newPath)):
            g_file.createDirectory(newPath, True, permission)
        g_file.changeMode(permission, ownerPath, True)
        if (user != ""):
            g_file.changeOwner(user, ownerPath, True)

        # check enter permission
        if ((user != "") and (os.getuid() == 0)):
            g_file.cdDirectory(newPath, user)

    @staticmethod
    def obtainInstStr(objectList):
        """
        function : Obtain the message from the objectList
        input : List
        output : String
        """
        info = ""
        if (isinstance(objectList, types.ListType)):
            for obj in objectList:
                info += "%s\n" % str(obj)
        return info

    @staticmethod
    def findUnsupportedParameters(parameterList):
        """
        function : find unsupported configuration parameters,
        just ignore other invalid parameters.
                   if don't find any unsupported configuration
                   parameter, return [].
        input : List
        output : []
        """
        # init unsupported args list
        unsupportedArgs = ["support_extended_features"]
        inputedUnsupportedParameters = []
        for param in parameterList:
            # split it by '='
            keyValue = param.split("=")
            if (len(keyValue) != 2):
                continue
            if (keyValue[0].strip() in unsupportedArgs):
                inputedUnsupportedParameters.append(param)

        return inputedUnsupportedParameters

    @staticmethod
    def judgePathUser(tempPath):
        """
        function: judge the owner of path if exist
        input: tempPath
        output: True/False
        """
        try:
            tempName = pwd.getpwuid(os.stat(tempPath).st_uid).pw_name
            return True
        except Exception as e:
            # if the user is not exist
            if (str(e).find("uid not found") >= 0):
                return False
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                                ("the owner of %s" % tempPath) +
                                " Error: \n%s" % str(e))

    @staticmethod
    def checkPathandChangeOwner(onePath, user, group, permission):
        """
        function: Get the owner of each layer path , if the user does not
                  exist and change owner
        input: onePath---the specified path; user---the user of cluster;
               group---the group of cluster
        output: the owner of path
        precondiftion: the path exists
        """
        pathlist = []
        try:
            if (not os.path.exists(onePath)):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % onePath)

            ownerPath = onePath
            while True:
                # obtain the each top path
                (ownerPath, dirName) = os.path.split(ownerPath)
                if (os.path.exists(ownerPath) and dirName != ""):
                    pathlist.append(os.path.join(ownerPath, dirName))
                else:
                    break

            for tempPath in pathlist:
                # the user does not exist
                if (not DefaultValue.judgePathUser(tempPath)):
                    g_file.changeMode(permission, tempPath)
                    g_file.changeOwner(user, tempPath)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkOsVersion():
        """
        function : Check os version
        input : NA
        output : boolean
        """
        # now we support this platform:
        #     RHEL/CentOS     "6.4", "6.5", "6.6", "6.7", "6.8", "6.9",
        #     "7.0", "7.1", "7.2", "7.3", "7.4", "7.5"64bit
        #     SuSE11  sp1/2/3/4 64bit
        #     EulerOS '2.0'64bit
        #     SuSE12  sp0/1/2/3 64bit
        try:
            g_Platform.getCurrentPlatForm()
            return True
        except Exception as e:
            return False

    @staticmethod
    def checkPreInstallFlag(user):
        """
        function : check if have called preinstall.py script
        input : String
        output : boolean
        """
        gaussEnv = DefaultValue.getEnvironmentParameterValue("GAUSS_ENV", user)
        if ("" == gaussEnv):
            return False
        if (str(gaussEnv) != "1" or str(gaussEnv) != "2"):
            return True
        else:
            return False

    @staticmethod
    def cleanTmpFile(path, fp=None):
        """
        function : close and remove temporary file
        input : String,file
        output : NA
        """
        if (fp):
            fp.close()
        if (os.path.exists(path)):
            os.remove(path)

    @staticmethod
    def distributeEncryptFiles(appPath, hostList):
        """
        function : distribute encrypted files of server.key.cipher
        and server.key.rand to remote host
        input : String,[]
        output : NA
        """
        # init encrypt file
        binPath = "%s/bin" % appPath
        encryptFile1 = "%s/server.key.cipher" % binPath
        encryptFile2 = "%s/server.key.rand" % binPath
        if (not os.path.exists(encryptFile1) or not os.path.exists(
                encryptFile2)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                            "encrypt files" + " Please check it.")
        # get user and group
        (user, group) = g_OSlib.getPathOwner(appPath)
        if (user == "" or group == ""):
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50308"] + ".")

        # copy encrypt file to host
        for host in hostList:
            targetPath = "'%s'/" % binPath
            g_OSlib.scpFile(host, encryptFile1, targetPath)
            g_OSlib.scpFile(host, encryptFile2, targetPath)

            chownCmd1 = g_Platform.getChownCmd(user, group, encryptFile1)
            chownCmd2 = g_Platform.getChownCmd(user, group, encryptFile2)
            chmodCmd1 = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                               encryptFile1)
            chmodCmd2 = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                               encryptFile2)
            executeCmd = "%s && %s && %s && %s" % (chownCmd1,
                                                   chownCmd2,
                                                   chmodCmd1,
                                                   chmodCmd2)
            cmd = g_OSlib.getSshCommand(host, executeCmd)
            DefaultValue.execCommandLocally(cmd)

    @staticmethod
    def distributeDatasourceFiles(sshTool, appPath, hostList):
        """
        function : distribute datasource files of datasource.key.cipher
                and datasource.key.rand to remote host
        input : String,String
        output : NA
        """
        # init datasource file
        clusterBinPath = "%s/bin" % appPath
        datasourceCipherFile = "%s/datasource.key.cipher" % clusterBinPath
        datasourceRandFile = "%s/datasource.key.rand" % clusterBinPath
        tde_key_cipher = "%s/gs_tde_keys.cipher" % clusterBinPath

        # If the file exists. Remote copy datasource cipher file to new nodes.
        if (os.path.isfile(datasourceCipherFile)):
            sshTool.scpFiles(datasourceCipherFile, clusterBinPath, hostList)
            cmd = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                         datasourceCipherFile)
            sshTool.executeCommand(
                cmd, "change the datasource cipher file permission",
                DefaultValue.SUCCESS, hostList)
        # If the file exists. Remote copy datasource rand file to new nodes.
        if (os.path.isfile(datasourceRandFile)):
            sshTool.scpFiles(datasourceRandFile, clusterBinPath, hostList)
            cmd = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                         datasourceRandFile)
            sshTool.executeCommand(cmd, "change the datasource "
                                        "rand file permission",
                                   DefaultValue.SUCCESS, hostList)
        # If the file exists. Remote copy gs_tde_keys.cipher to new nodes.
        if (os.path.isfile(tde_key_cipher)):
            sshTool.scpFiles(tde_key_cipher, clusterBinPath, hostList)
            cmd = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                         tde_key_cipher)
            sshTool.executeCommand(cmd, "change the gs_tde_keys.cipher "
                                        "permission", DefaultValue.SUCCESS,
                                   hostList)

    @staticmethod
    def distributeUtilslibDir(sshTool, user, appPath, hostList):
        """
        function : distribute utilslib dir to remote host
        input : String,String
        output : NA
        """
        localHostName = DefaultValue.GetHostIpOrName()
        # init utilslib dir
        datasourceLibPath = "%s/utilslib" % appPath
        if (os.path.exists(datasourceLibPath)):
            srcPath = "'%s'/*" % datasourceLibPath
            destPath = "'%s'/" % datasourceLibPath
            sshTool.scpFiles(srcPath, destPath, hostList)

        # init java UDF lib dir
        javaUDFLibPath = "%s/lib/postgresql/java" % appPath
        if (os.path.isdir(javaUDFLibPath)):
            udfFiles = g_file.getDirectoryList(javaUDFLibPath)
            if (len(udfFiles) > 0):
                srcPath = "'%s'/*" % javaUDFLibPath
                destPath = "'%s'/" % javaUDFLibPath
                sshTool.scpFiles(srcPath, destPath, hostList)

        # init postgis lib dir
        fileLocation = {}
        fileLocation["'%s'/lib/postgresql/" % appPath] = "postgis-*.*.so"
        fileLocation["'%s'/lib/" % appPath] = \
            "(libgeos_c.so.*|libproj.so.*|libjson-c.so.*|" \
            "libgeos-*.*.*so|libstdc++.*|libgcc_s.so.*)"
        fileLocation["'%s'/share/postgresql/extension/" % appPath] = \
            "(postgis--*.*.*.sql|postgis.control)"
        fileLocation["'%s'/bin/" % appPath] = "(pgsql2shp|shp2pgsql|" \
                                              "logic_cluster_name.txt|" \
                                              "[a-zA-Z0-9_]{1,64}." \
                                              "cluster_static_config)"
        fileLocation["'%s'/etc/" % appPath] = "*.gscgroup_.*.cfg"
        for (gisLibPath, pattarn) in fileLocation.items():
            gisFiles = g_file.getDirectoryList(gisLibPath, pattarn)
            if (len(gisFiles) > 0):
                if (len(gisFiles) > 1):
                    srcPath = "%s/{%s}" % (gisLibPath, ",".join(gisFiles))
                else:
                    srcPath = "%s/%s" % (gisLibPath, gisFiles[0])
                sshTool.scpFiles(srcPath, destPath, hostList)

    @staticmethod
    def distributeRackFile(sshTool, hostList):
        """
        function: Distributing the rack Information File
        input : NA
        output: NA
        """
        rack_conf_file = os.path.realpath(os.path.join(
            DefaultValue.getEnv("GPHOME"),
            "script/gspylib/etc/conf/rack_info.conf"))
        rack_info_temp = os.path.realpath(os.path.join(
            DefaultValue.getEnv("GPHOME"),
            "script/gspylib/etc/conf/rack_temp.conf"))
        if os.path.isfile(rack_info_temp):
            shutil.move(rack_info_temp, rack_conf_file)
        if os.path.isfile(rack_conf_file):
            sshTool.scpFiles(rack_conf_file, rack_conf_file, hostList)

    @staticmethod
    def cleanFile(fileName, hostname=""):
        """
        function : remove file
        input : String,hostname
        output : NA
        """
        fileList = fileName.split(",")

        cmd = ""
        for fileName in fileList:
            deleteCmd = g_file.SHELL_CMD_DICT["deleteFile"] % (fileName,
                                                               fileName)
            if cmd != "":
                cmd += ';%s' % deleteCmd
            else:
                cmd = deleteCmd

        if ("" != hostname and DefaultValue.GetHostIpOrName() != hostname):
            cmd = g_OSlib.getSshCommand(hostname, cmd)
        DefaultValue.execCommandLocally(cmd)

    @staticmethod
    def cleanUserEnvVariable(userProfile, cleanGAUSS_WARNING_TYPE=False,
                             cleanGS_CLUSTER_NAME=True):
        """
        function : Clean the user environment variable
        input : String,boolean 
        output : NA
        """
        try:
            # check use profile
            if os.path.isfile(userProfile):
                # clean version
                g_file.deleteLine(userProfile, "^\\s*export\\"
                                               "s*GAUSS_VERSION=.*$")
                # clean lib
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*LD_LIBRARY_PATH=\\"
                                  "$GAUSSHOME\\/lib:\\$LD_LIBRARY_PATH$")
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*LD_LIBRARY_PATH=\\"
                                  "$GAUSSHOME\\/lib\\/libsimsearch:\\"
                                  "$LD_LIBRARY_PATH$")
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*LD_LIBRARY_PATH=\\$GPHOME\\"
                                  "/script\\/gspylib\\/clib:\\"
                                  "$LD_LIBRARY_PATH$")
                # clean bin
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*PATH=\\$GAUSSHOME\\"
                                  "/bin:\\$PATH$")
                # clean GAUSSHOME
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*GAUSSHOME=.*$")
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*PGHOST=.*$")
                # clean GAUSSLOG
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*GAUSSLOG=.*$")
                # clean S3_ACCESS_KEY_ID
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*S3_ACCESS_KEY_ID=.*$")
                # clean S3_SECRET_ACCESS_KEY
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*S3_SECRET_ACCESS_KEY=.*$")
                # clean S3_CLIENT_CRT_FILE
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*S3_CLIENT_CRT_FILE=.*$")
                # clean ETCD_UNSUPPORTED_ARCH
                g_file.deleteLine(userProfile,
                                  "^\\s*export\\s*ETCD_UNSUPPORTED_ARCH=.*$")

                if (cleanGAUSS_WARNING_TYPE):
                    # clean extension connector environment variable
                    # because only deleting env_ec in postinstall, put it with
                    # GAUSS_WARNING_TYPE
                    g_file.deleteLine(userProfile, "^if \[ -f .*\/env_ec")
                    # clean GAUSS_WARNING_TYPE
                    g_file.deleteLine(userProfile, "^\\s*export\\"
                                                   "s*GAUSS_WARNING_TYPE=.*$")

                if (cleanGS_CLUSTER_NAME):
                    # clean GS_CLUSTER_NAME
                    g_file.deleteLine(userProfile, "^\\s*export\\"
                                                   "s*GS_CLUSTER_NAME=.*$")

                # clean AGENTPATH
                g_file.deleteLine(userProfile, "^\\s*export\\s*AGENTPATH=.*$")
                # clean AGENTLOGPATH
                g_file.deleteLine(userProfile, "^\\s*export\\s*AGENTLOGPATH="
                                               ".*$")
                # clean umask
                g_file.deleteLine(userProfile, "^\\s*umask\\s*.*$")

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def setComponentEnvVariable(userProfile, envList):
        """
        funciton: Set component environment variable
        input: userProfile- env file, envList - environment variable list
        output: NA
        """
        try:
            g_file.createFileInSafeMode(userProfile)
            with open(userProfile, "a") as fp:
                for inst_env in envList:
                    fp.write(inst_env)
                    fp.write(os.linesep)
                fp.flush()
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            userProfile + " Error: \n%s" % str(e))

    @staticmethod
    def setUserEnvVariable(userProfile, installPath, tmpPath, logPath,
                           agentPath, agentLogPath):
        """
        function : Set the user environment variable
        input : String,String,String,String,String,String
        output : NA
        """
        envList = ["export GAUSSHOME=%s" % installPath, \
                   "export PATH=$GAUSSHOME/bin:$PATH", \
                   "export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH", \
                   "export S3_CLIENT_CRT_FILE=$GAUSSHOME/lib/client.crt", \
                   "export GAUSS_VERSION=%s" %
                   VersionInfo.getPackageVersion(), \
                   "export PGHOST=%s" % tmpPath, \
                   "export GAUSSLOG=%s" % logPath,
                   "umask 077"]
        if agentPath != '':
            envList.append("export AGENTPATH=%s" % agentPath)
        if agentLogPath != '':
            envList.append("export AGENTLOGPATH=%s" % agentLogPath)
        DefaultValue.setComponentEnvVariable(userProfile, envList)

    @staticmethod
    def cleanComponentEnvVariable(userProfile, envNames):
        """
        function : Clean the user environment variable
        input : String,boolean 
        output : NA
        """
        try:
            if (os.path.exists(userProfile) and os.path.isfile(userProfile)):
                for envName in envNames:
                    g_file.deleteLine(userProfile, "^\\s*export\\s*%s=.*$" %
                                      envName)
                    if (envName == "GAUSSHOME"):
                        g_file.deleteLine(userProfile,
                                          "^\\s*export\\s*LD_LIBRARY_PATH"
                                          "=\\$GAUSSHOME\\/lib:"
                                          "\\$LD_LIBRARY_PATH$")
                        g_file.deleteLine(userProfile,
                                          "^\\s*export\\s*LD_LIBRARY_PATH"
                                          "=\\$GAUSSHOME\\/add-ons:"
                                          "\\$LD_LIBRARY_PATH$")
                        # clean bin
                        g_file.deleteLine(userProfile,
                                          "^\\s*export\\s*PATH"
                                          "=\\$GAUSSHOME\\/bin:\\$PATH$")
                    elif (envName == "CM_HOME"):
                        # clean cm path
                        g_file.deleteLine(userProfile,
                                          "^\\s*export\\s*PATH"
                                          "=\\$CM_HOME:\\$PATH$")
                    elif (envName == "ETCD_HOME"):
                        # clean etcd path
                        g_file.deleteLine(
                            userProfile, "^\\s*export\\s*PATH"
                                         "=\\$ETCD_HOME\\/bin:\\$PATH$")

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def updateUserEnvVariable(userProfile, variable, value):
        """
        function : Update the user environment variable
        input : String,String,String
        output : NA
        """
        try:
            # delete old env information
            deleteContent = "^\\s*export\\s*%s=.*$" % variable
            g_file.deleteLine(userProfile, deleteContent)
            # write the new env information into userProfile
            writeContent = ['export %s=%s' % (variable, value)]
            g_file.writeFile(userProfile, writeContent)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def createCADir(sshTool, caDir, hostList):
        """
        function : create the dir of ca file
        input : config file path and ca dir path
        output : NA
        """
        opensslFile = os.path.join(caDir, "openssl.cnf")
        tmpFile = os.path.join(os.path.realpath(
            os.path.join(caDir, "..")), "openssl.cnf")
        if (not os.path.isfile(opensslFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % opensslFile)

        # not rename file, just move it out and clean the dir, then move back
        cmd = g_file.SHELL_CMD_DICT["renameFile"] % (opensslFile,
                                                     opensslFile,
                                                     tmpFile)
        cmd += " && " + g_file.SHELL_CMD_DICT["cleanDir"] % (caDir,
                                                             caDir,
                                                             caDir)
        cmd += " && " + g_file.SHELL_CMD_DICT["renameFile"] % (tmpFile,
                                                               tmpFile,
                                                               opensslFile)
        sshTool.executeCommand(cmd, "move file and clean dir",
                               DefaultValue.SUCCESS, hostList)
        # create ./demoCA/newcerts ./demoCA/private
        newcertsPath = os.path.join(caDir, "demoCA/newcerts")
        g_file.createDirectory(newcertsPath)
        privatePath = os.path.join(caDir, "demoCA/private")
        g_file.createDirectory(privatePath)
        # touch files: ./demoCA/serial ./demoCA/index.txt
        serFile = os.path.join(caDir, "demoCA/serial")
        g_file.createFile(serFile)
        g_file.writeFile(serFile, ["01"])
        indexFile = os.path.join(caDir, "demoCA/index.txt")
        g_file.createFile(indexFile)

    @staticmethod
    def createServerCA(caType, caDir, logger):
        """
        function : create ca file
        input : ca file type and ca dir path
        output : NA
        """
        if (caType == DefaultValue.SERVER_CA):
            logger.log("The sslcert will be generated in %s" % caDir)
            randpass = DefaultValue.getRandStr()
            confFile = caDir + "/openssl.cnf"
            if not os.path.isfile(confFile):
                raise Exception(ErrorCode.GAUSS_502
                                ["GAUSS_50201"] % confFile)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256  -passout pass:%s -out " % \
                   (randpass)
            cmd += "demoCA/private/cakey.pem 2048"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf -new "
            cmd += "-key demoCA/private/cakey.pem -passin pass:%s " \
                   "-out " % (randpass)
            cmd += "demoCA/careq.pem -subj "
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=root'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            g_file.replaceFileLineContent("CA:FALSE",
                                          "CA:TRUE",
                                          confFile)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf "
            cmd += "-batch -passin pass:%s -out demoCA/cacert.pem " \
                   "-keyfile " % (randpass)
            cmd += "demoCA/private/cakey.pem "
            cmd += "-selfsign -infiles demoCA/careq.pem "
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256 -passout pass:%s -out " \
                   "server.key 2048" % (randpass)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf -new "
            cmd += "-key server.key -passin pass:%s -out server.req " \
                   "-subj " % (randpass)
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=server'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            g_file.replaceFileLineContent("CA:TRUE",
                                          "CA:FALSE",
                                          confFile)
            indexAttrFile = caDir + "/demoCA/index.txt.attr"
            if os.path.isfile(indexAttrFile):
                g_file.replaceFileLineContent("unique_subject = yes",
                                              "unique_subject = no",
                                              indexAttrFile)
            else:
                raise Exception(ErrorCode.GAUSS_502
                                ["GAUSS_50201"] % indexAttrFile)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf -batch -in "
            cmd += "server.req -passin pass:%s -out server.crt " \
                   "-days 3650 -md sha256 -subj " % (randpass)
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=server'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && gs_guc encrypt -M server -K %s -D ./ " % randpass
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            # client key
            randpassClient = DefaultValue.getRandStr()
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256  -passout pass:%s -out " \
                   "client.key 2048" % (randpassClient)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf "
            cmd += "-new -key client.key -passin pass:%s " \
                   "-out client.req -subj " % (randpassClient)
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=client'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf "
            cmd += "-batch -in client.req  -passin pass:%s -out " % \
                   (randpass)
            cmd += "client.crt -days 3650 -md sha256"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && gs_guc encrypt -M client -K %s -D ./ " % randpassClient
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl pkcs8 -topk8 -outform DER"
            cmd += " -passin pass:%s  " % randpassClient
            cmd += " -in client.key -out client.key.pk8 -nocrypt"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            randpass = ""
            randpassClient = ""

    @staticmethod
    def changeOpenSslConf(confFile, hostList):
        """
        function : change the openssl.cnf file
        input : confFile, hostList
        output : NA
        """
        # Clean the old content.
        lineList = g_file.readFile(confFile)
        for i in range(len(lineList)):
            if ("[" in lineList[i] and
                    "alt_names" in lineList[i] and
                    "]" in lineList[i]):
                row = i + 1
                g_file.deleteLineByRowNum(confFile, row)
            if ("DNS." in lineList[i] and "=" in lineList[i]):
                g_file.deleteLineByRowNum(confFile, row)
        # Add new one.
        dnsList = []
        dnsList.append("\n")
        dnsList.append("[ alt_names ]")
        dnsList.append("DNS.1 = localhost")
        cont = 2
        for host in hostList:
            dns = "DNS." + str(cont) + " = " + host
            dnsList.append(dns)
            cont = cont + 1
        g_file.writeFile(confFile, dnsList)

    @staticmethod
    def getRandStr():
        with open("/dev/random", 'rb') as fp:
            srp = fp.read(4)
            salt = srp.hex()
            salt = "%s%s" % (salt, "aA0")
        return salt

    @staticmethod
    def createCA(caType, caDir):
        """
        function : create ca file
        input : ca file type and ca dir path
        output : NA
        """
        if (caType == DefaultValue.GRPC_CA):
            randpass = DefaultValue.getRandStr()
            confFile = caDir + "/openssl.cnf"
            if (os.path.isfile(confFile)):
                g_file.replaceFileLineContent("cakey.pem",
                                              "cakeynew.pem",
                                              confFile)
                g_file.replaceFileLineContent("careq.pem",
                                              "careqnew.pem",
                                              confFile)
                g_file.replaceFileLineContent("cacert.pem",
                                              "cacertnew.pem",
                                              confFile)
                g_file.replaceFileLineContent("server.key",
                                              "servernew.key",
                                              confFile)
                g_file.replaceFileLineContent("server.req",
                                              "servernew.req",
                                              confFile)
                g_file.replaceFileLineContent("server.crt",
                                              "servernew.crt",
                                              confFile)
                g_file.replaceFileLineContent("client.key",
                                              "clientnew.key",
                                              confFile)
                g_file.replaceFileLineContent("client.req",
                                              "clientnew.req",
                                              confFile)
                g_file.replaceFileLineContent("client.crt",
                                              "clientnew.crt",
                                              confFile)
            else:
                raise Exception(ErrorCode.GAUSS_502
                                ["GAUSS_50201"] % confFile)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256  -passout pass:%s -out " % \
                   (randpass)
            cmd += "demoCA/private/cakeynew.pem 2048"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf -new "
            cmd += "-key demoCA/private/cakeynew.pem -passin pass:%s " \
                   "-out " % (randpass)
            cmd += "demoCA/careqnew.pem -subj "
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=root'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf -days 7300 "
            cmd += "-batch -passin pass:%s -out demoCA/cacertnew.pem " \
                   "-md sha512 -keyfile " % (randpass)
            cmd += "demoCA/private/cakeynew.pem "
            cmd += "-selfsign -infiles demoCA/careqnew.pem "
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256 -passout pass:%s -out " \
                   "servernew.key 2048" % (randpass)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf -new "
            cmd += "-key servernew.key  -passin pass:%s -out servernew.req " \
                   "-subj " % (randpass)
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=root'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            indexAttrFile = caDir + "/demoCA/index.txt.attr"
            if (os.path.isfile(indexAttrFile)):
                g_file.replaceFileLineContent("unique_subject = yes",
                                              "unique_subject = no",
                                              indexAttrFile)
            else:
                raise Exception(ErrorCode.GAUSS_502
                                ["GAUSS_50201"] % indexAttrFile)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf -batch -in "
            cmd += "servernew.req -passin pass:%s -out servernew.crt " \
                   "-days 7300 -md sha512" % (randpass)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl genrsa -aes256  -passout pass:%s -out " \
                   "clientnew.key 2048" % (randpass)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl req -config openssl.cnf "
            cmd += "-new -key clientnew.key -passin pass:%s " \
                   "-out clientnew.req -subj " % (randpass)
            cmd += "'/C=CN/ST=Beijing/L=Beijing/"
            cmd += "O=huawei/OU=gauss/CN=root'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && openssl ca -config openssl.cnf "
            cmd += "-batch -in clientnew.req  -passin pass:%s -out " % \
                   (randpass)
            cmd += "clientnew.crt -days 7300 -md sha512"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && gs_guc encrypt -M server -K %s -D ./ " % randpass
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            cmd = g_Platform.getCdCmd(caDir)
            cmd += " && gs_guc encrypt -M client -K %s -D ./ " % randpass
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514
                                ["GAUSS_51402"] + "Error:\n%s" % output)
            randpass = 0

    @staticmethod
    def cleanServerCaDir(caDir):
        """
        function : clean the dir of ca file and change mode of ca files
        input : ca dir path
        output : NA
        """
        certFile = caDir + "/demoCA/cacert.pem"
        if os.path.exists(certFile):
            g_file.moveFile(certFile, caDir)
        clientReq = caDir + "/server.req"
        g_file.removeFile(clientReq)
        clientReq = caDir + "/client.req"
        g_file.removeFile(clientReq)
        demoCA = caDir + "/demoCA"
        g_file.removeDirectory(demoCA)
        allCerts = caDir + "/*"
        g_file.changeMode(DefaultValue.KEY_FILE_MODE, allCerts)

    @staticmethod
    def cleanCaDir(caDir):
        """
        function : clean the dir of ca file and change mode of ca files
        input : ca dir path
        output : NA
        """
        certFile = caDir + "/demoCA/cacertnew.pem"
        if os.path.exists(certFile):
            g_file.moveFile(certFile, caDir)
        clientReq = caDir + "/clientnew.req"
        g_file.removeFile(clientReq)
        clientReq = caDir + "/servernew.req"
        g_file.removeFile(clientReq)
        demoCA = caDir + "/demoCA"
        g_file.removeDirectory(demoCA)
        allCerts = caDir + "/*"
        g_file.changeMode(DefaultValue.KEY_FILE_MODE, allCerts)

    @staticmethod
    def modifyFileOwner(user, currentfile):
        """
        function : Modify the file's owner
        input : String,String
        output : String
        """
        # only root user can run this function
        if (os.getuid() == 0):
            try:
                group = g_OSlib.getGroupByUser(user)
            except Exception as e:
                raise Exception(str(e))
            if os.path.exists(currentfile):
                g_file.changeOwner(user, currentfile)

    @staticmethod
    def modifyFileOwnerFromGPHOME(currentfile):
        """
        function : Modify the file's owner to the GPHOME's user
        input : String,String
        output : String
        """
        GPHOME = DefaultValue.getEnv(DefaultValue.TOOL_PATH_ENV)
        if not GPHOME:
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] % "GPHOME")
        (user, group) = g_OSlib.getPathOwner(GPHOME)
        if (user == "" or group == ""):
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50308"])
        DefaultValue.modifyFileOwner(user, currentfile)

    @staticmethod
    def obtainSSDDevice():
        """
        function : Obtain the SSD device
        input : NA
        output : []
        """
        devList = []
        cmd = "ls -ll /dev/hio? | awk '{print $10}'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0 and output.find("No such file or directory") < 0):
            devList = output.split("\n")
        else:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53005"] +
                            " Command:%s. Error:\n%s" % (cmd, output))
        return devList

    @staticmethod
    def checkOutputFile(outputFile):
        """
        function : check the output file
        input : String
        output : NA
        """
        if (os.path.isdir(outputFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % "output file")
        # get parent directory of output file
        parent_dir = os.path.dirname(outputFile)
        if (os.path.isfile(parent_dir)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50211"] %
                            "base directory of output file")

    @staticmethod
    def getAllIP(g_dbNodes):
        """
        function : Get all node IP
        input : list
        output : list
        """
        allIP = []
        for dbNode in g_dbNodes:
            allIP += dbNode.backIps
            allIP += dbNode.sshIps
            for dbInstance in dbNode.cmservers:
                allIP += dbInstance.haIps
                allIP += dbInstance.listenIps
            for dbInstance in dbNode.coordinators:
                allIP += dbInstance.haIps
                allIP += dbInstance.listenIps
            for dbInstance in dbNode.datanodes:
                allIP += dbInstance.haIps
                allIP += dbInstance.listenIps
            for dbInstance in dbNode.gtms:
                allIP += dbInstance.haIps
                allIP += dbInstance.listenIps
            for etcdInst in dbNode.etcds:
                allIP += etcdInst.haIps
                allIP += etcdInst.listenIps

        return allIP

    @staticmethod
    def KillAllProcess(userName, procName):
        """
        function : Kill all processes by userName and procName.
        input : userName, procName
        output : boolean
        """
        return g_OSlib.killallProcess(userName, procName, "9")

    @staticmethod
    def sendNetworkCmd(ip):
        """
        function : Send the network command of ping. 
        input : String
        output : NA
        """
        cmd = "%s |%s ttl |%s -l" % (g_Platform.getPingCmd(ip, "5", "1"),
                                     g_Platform.getGrepCmd(),
                                     g_Platform.getWcCmd())
        (status, output) = subprocess.getstatusoutput(cmd)
        if (str(output) == '0' or status != 0):
            g_lock.acquire()
            noPassIPs.append(ip)
            g_lock.release()

    @staticmethod
    def checkIsPing(ips):
        """
        function : Check the connection status of network.
        input : []
        output : []
        """
        global noPassIPs
        noPassIPs = []
        results = parallelTool.parallelExecute(DefaultValue.sendNetworkCmd,
                                               ips)
        return noPassIPs

    @staticmethod
    def retryGetstatusoutput(cmd, retryTime=3, sleepTime=1):
        """
        function : retry getStatusoutput
        @param cmd: command  going to be execute
        @param retryTime: default retry 3 times after execution failure
        @param sleepTime: default sleep 1 second then start retry 
        """
        retryTime += 1
        for i in range(retryTime):
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                time.sleep(sleepTime)
            else:
                break
        return status, output

    @staticmethod
    def killInstProcessCmd(instName, isRemote=False, signal=9,
                           isExactMatch=True, instType="",
                           procAbsPath="", instDir=""):
        """
        instName: process name
        isRemote: do it under remote machine. default is false
        signal  : kill signle. default is 9 
        isExactMatch: the match rule. default is exact match
        instType: instance type. default is "", now only support for get
        coordinator instance
        procAbsPath: process abs path. default is ""
        instDir: instance data directory. default is ""
        """
        pstree = "python3 %s -sc" % os.path.realpath(os.path.dirname(
            os.path.realpath(__file__)) + "/../../py_pstree.py")
        # only cm_server need kill all child process, when do kill -9
        if instName == "cm_server" and signal == 9:
            if isRemote:
                cmd = "pidList=\`ps ux | grep '\<cm_server\>' | grep -v " \
                      "'grep' " \
                      "| awk '{print \$2}' | xargs \`; for pid in \$pidList;" \
                      " do %s \$pid | xargs -r -n 100 kill -9; echo " \
                      "'SUCCESS'; " \
                      "done" % pstree
                # only try to kill -9 process of cmserver
                cmd += "; ps ux | grep '\<cm_server\>' | grep -v grep | awk " \
                       "'{print \$2}' | xargs -r kill -9; echo 'SUCCESS'"
            else:
                cmd = "pidList=`ps ux | grep '\<cm_server\>' | grep -v " \
                      "'grep' |" \
                      " awk '{print $2}' | xargs `; for pid in $pidList; " \
                      "do %s $pid | xargs -r -n 100 kill -9; echo 'SUCCESS';" \
                      " done" % pstree
                cmd += "; ps ux | grep '\<cm_server\>' | grep -v grep | " \
                       "awk '{print $2}' | xargs -r kill -9; echo 'SUCCESS'"
            return cmd

        if "" != instType and "" != procAbsPath and "" != instDir:
            if isRemote:
                cmd = "ps ux | grep '\<%s\>' | grep '%s' | grep '%s' | " \
                      "grep -v grep | awk '{print \$2}' | xargs -r kill -%d " \
                      "" % \
                      (instType, procAbsPath, instDir, signal)
            else:
                cmd = "ps ux | grep '\<%s\>' | grep '%s' | grep '%s' | " \
                      "grep -v grep | awk '{print $2}' | xargs -r kill -%d " \
                      % \
                      (instType, procAbsPath, instDir, signal)
        else:
            if (isExactMatch):
                if (isRemote):
                    cmd = "ps ux | grep '\<%s\>' | grep -v grep | awk " \
                          "'{print \$2}' | xargs -r kill -%d " % (instName,
                                                                  signal)
                else:
                    cmd = "ps ux | grep '\<%s\>' | grep -v grep | awk " \
                          "'{print $2}' | xargs -r kill -%d " % (instName,
                                                                 signal)
            else:
                if (isRemote):
                    cmd = "ps ux | grep '%s' | grep -v grep | awk " \
                          "'{print \$2}' | xargs -r kill -%d " % (instName,
                                                                  signal)
                else:
                    cmd = "ps ux | grep '%s' | grep -v grep | " \
                          "awk '{print $2}' | xargs -r kill -%d " % (instName,
                                                                     signal)
        return cmd

    @staticmethod
    def getRuningInstNum(procAbsPath, instDir=""):
        """
        """
        if (instDir):
            cmd = "ps ux | grep '%s' | grep '%s' | grep -v grep | wc -l" % \
                  (procAbsPath, instDir)
        else:
            cmd = "ps ux | grep '%s' | grep -v grep | wc -l" % (procAbsPath)
        return cmd

    @staticmethod
    def killCmserverProcess(sshTool, cmsInsts):
        # Restart the instance CMSERVERS
        failedNodes = []
        if (len(cmsInsts) == 1 and cmsInsts[0].hostname ==
                DefaultValue.GetHostIpOrName()):
            cmd = DefaultValue.killInstProcessCmd("cm_server", False, 1)
            (status, output) = DefaultValue.retryGetstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error:\n%s" % output)
        else:
            cmd = DefaultValue.killInstProcessCmd("cm_server", True, 1)
            (status, output) = sshTool.getSshStatusOutput(
                cmd, [cmsInst.hostname for cmsInst in cmsInsts])
            for cmNodeName in status.keys():
                if (status[cmNodeName] != DefaultValue.SUCCESS):
                    failedNodes.append(cmNodeName)

            # judge failed nodes
            if (len(failedNodes)):
                time.sleep(1)
                (status, output) = sshTool.getSshStatusOutput(cmd, failedNodes)
                for cmNodeName in failedNodes:
                    if (status[cmNodeName] != DefaultValue.SUCCESS):
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                        % cmd + "Error:\n%s" % output)

        time.sleep(10)

    @staticmethod
    def getParaValueFromConfigFile(paraList, instList, instType="cm_server"):
        """
        function : Get guc parameter from config file for cm_server or gtm.
        input : paraList, instList, instType
        output : paraMap
        """
        paraMap = {}
        for para in paraList:
            for inst in instList:
                configPath = os.path.join(inst.datadir, "%s.conf" % instType)
                (status, output) = DefaultValue.getMatchingResult(
                    "\<'%s'\>" % para, configPath, inst.hostname)
                if (status != 0 and status != 256):
                    if (instType == "gtm"):
                        output = ""
                    else:
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                                        configPath + " Error:%s." % output)
                configValue = ""
                for line in output.split('\n'):
                    confInfo = line.strip()
                    if (confInfo.startswith('#') or confInfo == ""):
                        continue
                    elif (confInfo.startswith(para)):
                        configValue = \
                            confInfo.split('#')[0].split('=')[
                                1].strip().lower()
                        if (paraMap.__contains__(para) and paraMap[para] !=
                                configValue):
                            raise Exception(
                                ErrorCode.GAUSS_530["GAUSS_53011"] %
                                "Parameter '%s', it is different in "
                                "same level instance." % para)
                        paraMap[para] = configValue
                        break
        return paraMap

    @staticmethod
    def retry_gs_guc(cmd):
        """
        function : Retry 3 times when HINT error
        input : cmd
        output : NA
        """
        retryTimes = 0
        while True:
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                break
            if (retryTimes > 1):
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50008"] +
                                " Command:%s. Error:\n%s" % (cmd, output))
            retryTimes = retryTimes + 1
            time.sleep(3)

    @staticmethod
    def distributePackagesToRemote(g_sshTool, srcPackageDir, destPackageDir,
                                   hostname=None, mpprcFile="",
                                   clusterInfo=None):
        '''
        function: distribute the package to remote nodes
        input: g_sshTool, hostname, srcPackageDir, destPackageDir, mpprcFile,
               clusterType
        output:NA
        '''
        if hostname is None:
            hostname = []
        try:
            # check the destPackageDir is existing on hostname
            DefaultValue.checkRemoteDir(g_sshTool, destPackageDir, hostname,
                                        mpprcFile)

            # Send compressed package to every host
            g_sshTool.scpFiles("%s/%s" % (
                srcPackageDir, DefaultValue.get_package_back_name()),
                               destPackageDir, hostname, mpprcFile)
            # Decompress package on every host
            srcPackage = "'%s'/'%s'" % (destPackageDir,
                                        DefaultValue.get_package_back_name())
            cmd = g_Platform.getDecompressFilesCmd(srcPackage, destPackageDir)
            g_sshTool.executeCommand(cmd, "extract %s server package" %
                                     VersionInfo.PRODUCT_NAME,
                                     DefaultValue.SUCCESS, hostname, mpprcFile)

            # change owner and mode of packages
            destPath = "'%s'/*" % destPackageDir
            cmd = g_Platform.getChmodCmd(str(DefaultValue.MAX_DIRECTORY_MODE),
                                         destPath, True)
            g_sshTool.executeCommand(cmd, "change permission",
                                     DefaultValue.SUCCESS, hostname, mpprcFile)

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def distributeTransEncryptFile(appPath, sshTool, hostList):
        '''
        function: Distribute trans encrypt file to the node of hostList
        input : appPath, sshTool, hostList
        output: NA
        '''
        try:
            installBinPath = "%s/bin" % appPath
            transEncryptKeyCipher = "%s/trans_encrypt.key.cipher" % \
                                    installBinPath
            transEncryptKeyRand = "%s/trans_encrypt.key.rand" % installBinPath
            transEncryptKeyAkSk = "%s/trans_encrypt_ak_sk.key" % installBinPath

            if (os.path.exists(transEncryptKeyCipher)):
                # MIN_FILE_MODE can not be scp, so expand the permission.
                cmd = g_Platform.getChmodCmd(
                    str(DefaultValue.KEY_DIRECTORY_MODE),
                    transEncryptKeyCipher)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))
                sshTool.scpFiles(transEncryptKeyCipher, installBinPath,
                                 hostList)
                cmd = g_Platform.getChmodCmd(
                    str(DefaultValue.MIN_FILE_MODE), transEncryptKeyCipher)
                sshTool.executeCommand(
                    cmd, "change permission", DefaultValue.SUCCESS, hostList)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))

            if (os.path.exists(transEncryptKeyRand)):
                # MIN_FILE_MODE can not be scp, so expand the permission.
                cmd = g_Platform.getChmodCmd(
                    str(DefaultValue.KEY_DIRECTORY_MODE), transEncryptKeyRand)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))
                sshTool.scpFiles(transEncryptKeyRand, installBinPath, hostList)
                cmd = g_Platform.getChmodCmd(str(DefaultValue.MIN_FILE_MODE),
                                             transEncryptKeyRand)
                sshTool.executeCommand(
                    cmd, "change permission", DefaultValue.SUCCESS, hostList)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))

            if (os.path.exists(transEncryptKeyAkSk)):
                # MIN_FILE_MODE can not be scp, so expand the permission.
                cmd = g_Platform.getChmodCmd(
                    str(DefaultValue.KEY_DIRECTORY_MODE), transEncryptKeyAkSk)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))
                sshTool.scpFiles(transEncryptKeyAkSk, installBinPath, hostList)
                cmd = g_Platform.getChmodCmd(str(DefaultValue.MIN_FILE_MODE),
                                             transEncryptKeyAkSk)
                sshTool.executeCommand(cmd, "change permission",
                                       DefaultValue.SUCCESS, hostList)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def distributeXmlConfFile(g_sshTool, confFile, hostname=None,
                              mpprcFile="", localMode=False):
        '''
        function: distribute the confFile to remote nodes
        input: g_sshTool, hostname, confFile, mpprcFile
        output:NA
        '''
        if hostname is None:
            hostname = []
        try:
            # distribute xml file
            # check and create xml file path
            xmlDir = os.path.dirname(confFile)
            xmlDir = os.path.normpath(xmlDir)
            DefaultValue.checkRemoteDir(g_sshTool, xmlDir, hostname, mpprcFile,
                                        localMode)
            local_node = DefaultValue.GetHostIpOrName()
            # Skip local file overwriting
            if not hostname:
                hostname = g_sshTool.hostNames[:]
            if local_node in hostname:
                hostname.remove(local_node)
            if (not localMode):
                # Send xml file to every host
                g_sshTool.scpFiles(confFile, xmlDir, hostname, mpprcFile)
            # change owner and mode of xml file
            cmd = g_Platform.getChmodCmd(str(DefaultValue.FILE_MODE), confFile)
            DefaultValue.execCommandWithMode(cmd,
                                             "change permission",
                                             g_sshTool,
                                             localMode,
                                             mpprcFile,
                                             hostname)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def cleanFileDir(dirName, g_sshTool=None, hostname=None):
        '''
        function: clean directory or file
        input: dirName, g_sshTool, hostname
        output:NA
        '''
        if hostname is None:
            hostname = []
        try:
            cmd = g_file.SHELL_CMD_DICT["deleteDir"] % (dirName, dirName)
            # If clean file or directory  on local node
            if (g_sshTool is None):
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % str(output))
            else:
                # Assign some remote node to clean directory or file.
                if hostname == []:
                    g_sshTool.executeCommand(cmd, "clean directory or file ")
                else:
                    g_sshTool.executeCommand(cmd, "clean directory or file ",
                                             DefaultValue.SUCCESS, hostname)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def execCommandLocally(cmd):
        """ 
        functino: exec only on local node
        input: cmd
        output: NA
        """
        # exec the cmd
        (status, output) = subprocess.getstatusoutput(cmd)
        # if cmd failed, then raise
        if (status != 0 and "[GAUSS-5" in str(output)):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error:\n%s" % str(output))
        elif (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % str(cmd) +
                            " Error: \n%s" % str(output))

    @staticmethod
    def execCommandWithMode(cmd, descript, g_sshTool, localMode=False,
                            mpprcFile='', hostList=None):
        """ 
        function: check the mode, if local mode, exec only on local node,
        else exec on all nodes
        input: cmd, decript, g_sshTool, localMode, mpprcFile
        output: NA
        """
        if hostList is None:
            hostList = []
        # check the localMode
        if localMode:
            # localMode
            DefaultValue.execCommandLocally(cmd)
        else:
            # Non-native mode
            g_sshTool.executeCommand(cmd, descript, DefaultValue.SUCCESS,
                                     hostList, mpprcFile)

    @staticmethod
    def getDevices():
        """
        functino: get device
        input: NA
        output: NA
        """
        cmd = "fdisk -l 2>/dev/null | grep \"Disk /dev/\" | " \
              "grep -Ev \"/dev/mapper/|loop\" | awk '{ print $2 }' | " \
              "awk -F'/' '{ print $NF }' | sed s/:$//g"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s" % output)
        devList = output.split('\n')
        return devList

    @staticmethod
    def copyCAfile(sshTool, hostList):
        """
        functino: copy CA file
        input: NA
        output: NA
        """
        try:
            user = pwd.getpwuid(os.getuid()).pw_name
            gaussHome = DefaultValue.getInstallDir(user)
            sslpath = "%s/share/sslcert/etcd/" % gaussHome
            caKeyFile = "%s/ca.key" % sslpath
            caCrtFile = "%s/etcdca.crt" % sslpath
            clientKeyFile = "%s/client.key" % sslpath
            clientCrtFile = "%s/client.crt" % sslpath
            etcdKeyRand = "%s/etcd.key.rand" % sslpath
            etcdKeyCipher = "%s/etcd.key.cipher" % sslpath
            clientKeyRand = "%s/client.key.rand" % sslpath
            clientKeyCipher = "%s/client.key.cipher" % sslpath

            if (os.path.exists(caKeyFile)):
                mkdirCmd = g_Platform.getMakeDirCmd(sslpath, True)
                changModeCmd = g_Platform.getChmodCmd(
                    str(DefaultValue.KEY_DIRECTORY_MODE), sslpath)
                cmd = "%s && %s" % (mkdirCmd, changModeCmd)
                sshTool.executeCommand(cmd, "create CA path",
                                       DefaultValue.SUCCESS, hostList)
                sshTool.scpFiles(caKeyFile, sslpath, hostList)
                sshTool.scpFiles(caCrtFile, sslpath, hostList)
                sshTool.scpFiles(clientKeyFile, sslpath, hostList)
                sshTool.scpFiles(clientCrtFile, sslpath, hostList)
                sshTool.scpFiles(etcdKeyRand, sslpath, hostList)
                sshTool.scpFiles(etcdKeyCipher, sslpath, hostList)
                sshTool.scpFiles(clientKeyRand, sslpath, hostList)
                sshTool.scpFiles(clientKeyCipher, sslpath, hostList)
                cmd = g_Platform.getChmodCmd(str(DefaultValue.KEY_FILE_MODE),
                                             "%s %s" % (caKeyFile, caCrtFile))
                sshTool.executeCommand(cmd, "change permission",
                                       DefaultValue.SUCCESS, hostList)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def genCert(nodeip, etcddir, remoteip=""):
        """
        function: generate a certificate file for ETCD
        input : nodeip:backip, etcddir: the dir of etcd, remoteip:sship
        output: NA
        """
        try:
            user = pwd.getpwuid(os.getuid()).pw_name
            ###############1.Save the openssl.cnf under
            # $GAUSSHOME/share/sslcert/etcd
            gaussHome = DefaultValue.getInstallDir(user)
            sslpath = "%s/share/sslcert/etcd" % gaussHome
            sslcfg = "%s/openssl.cnf" % sslpath
            tmp_Dir = "%s/demoCA" % sslpath
            etcdKeyFile = "%s/etcd.key" % sslpath
            etcdCsrFile = "%s/etcd.csr" % sslpath
            etcdCrtFile = "%s/etcd.crt" % sslpath
            etcdKeyRand = "%s/etcd.key.rand" % sslpath
            etcdKeyCipher = "%s/etcd.key.cipher" % sslpath

            ###############2.clean file
            DefaultValue.cleanFileDir(tmp_Dir)

            ###############3.generate server certificate and sign it
            # create directory and copy files
            ###############3.generate server certificate and sign it
            randpass = DefaultValue.aes_cbc_decrypt_with_path(sslpath)
            cmd = "%s" % g_Platform.getCdCmd(sslpath)
            # Create paths and files
            cmd += " && %s" % g_Platform.getMakeDirCmd("demoCA/newcerts", True)
            cmd += " && %s" % g_Platform.getMakeDirCmd("demoCA/private", True)
            cmd += " && %s" % g_Platform.getChmodCmd(
                str(DefaultValue.KEY_DIRECTORY_MODE), "demoCA/newcerts")
            cmd += " && %s" % g_Platform.getChmodCmd(
                str(DefaultValue.KEY_DIRECTORY_MODE), "demoCA/private")
            cmd += " && %s" % g_Platform.getTouchCmd("demoCA/index.txt")
            cmd += " && echo '01' > demoCA/serial"
            cmd += " && export SAN=\"IP:%s\"" % nodeip
            cmd += " && %s " % g_Platform.getCopyCmd("ca.key",
                                                     "demoCA/private/")
            cmd += " && %s " % g_Platform.getCopyCmd("etcdca.crt", "demoCA/")

            cmd += " && openssl req -config '%s' -newkey rsa:4096 -keyout " \
                   "'%s' -passout pass:%s -out '%s' -subj '/CN=cn'" % \
                   (sslcfg, etcdKeyFile, randpass, etcdCsrFile)
            cmd += " && %s" % g_Platform.getCdCmd("demoCA")
            cmd += " && openssl ca -startdate 200101000000Z -config " \
                   "'%s' -extensions etcd_server -batch -keyfile " \
                   "'%s/demoCA/private/ca.key' -passin pass:%s -cert " \
                   "'%s/demoCA/etcdca.crt' -out '%s' -infiles '%s'" % \
                   (sslcfg, sslpath, randpass, sslpath, etcdCrtFile,
                    etcdCsrFile)
            cmd += " && cd ../ && find . -type f | xargs chmod %s" % \
                   DefaultValue.KEY_FILE_MODE
            DefaultValue.execCommandLocally(cmd)
            ############4.copy etcd.srt to the ETCD directory
            # copy the file to the ETCD directory
            etcddir = "%s/" % etcddir
            if (remoteip):
                g_OSlib.scpFile(remoteip, etcdKeyFile, etcddir)
                g_OSlib.scpFile(remoteip, etcdCrtFile, etcddir)
                g_OSlib.scpFile(remoteip, etcdKeyRand, etcddir)
                g_OSlib.scpFile(remoteip, etcdKeyCipher, etcddir)
            else:
                g_file.cpFile(etcdKeyFile, etcddir)
                g_file.cpFile(etcdCrtFile, etcddir)
                g_file.cpFile(etcdKeyRand, etcddir)
                g_file.cpFile(etcdKeyCipher, etcddir)
            cmd = "unset SAN"
            DefaultValue.execCommandLocally(cmd)
            DefaultValue.cleanFileDir(tmp_Dir)
        except Exception as e:
            DefaultValue.cleanFileDir(tmp_Dir)
            raise Exception(str(e))

    @staticmethod
    def replaceCertFilesToRemoteNode(cnNodeName, instanceList, cnInstDir):
        """
        function: This method is for replace SSL cert files
        input : cnNodeName, instanceList, cnInstDir
        output: NA
        """
        fileList = DefaultValue.CERT_ROLLBACK_LIST[:]
        for file_inx in range(len(fileList)):
            fileList[file_inx] = os.path.join(cnInstDir, fileList[file_inx])

        # copy encrypt file to host
        for instInfo in instanceList:
            for certfile in fileList:
                # scp certfile from cnNodeName to instInfo.hostname
                sshCmd = g_Platform.getSshCmd(cnNodeName)
                scpCmd = g_Platform.getRemoteCopyCmd(certfile, "%s/" %
                                                     instInfo.datadir,
                                                     instInfo.hostname,
                                                     otherHost=cnNodeName)
                cmd = "%s \"if [ -f '%s' ]; then %s; fi\"" % (sshCmd, certfile,
                                                              scpCmd)
                DefaultValue.execCommandLocally(cmd)
                # change the certfile under instInfo.hostname
                sshCmd = g_Platform.getSshCmd(instInfo.hostname)
                chmodCmd = g_Platform.getChmodCmd(
                    str(DefaultValue.KEY_FILE_MODE), certfile)
                cmd = "%s \"if [ -f '%s' ]; then %s; fi\"" % (sshCmd, certfile,
                                                              chmodCmd)
                DefaultValue.execCommandLocally(cmd)

    @staticmethod
    def getSecurityMode():
        """
        function:to set security mode,if security_mode is not in config
                 file,return off.
        input:String
        output:String
        """
        securityModeValue = "off"
        try:
            cmd = "ps -ux | grep \"\\-\\-securitymode\" | grep -v \"grep\""
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 and output != "":
                raise Exception(
                    (ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                     + "Error: \n %s" % output))
            if output != "":
                securityModeValue = "on"
            return securityModeValue
        except Exception as ex:
            raise Exception(str(ex))

    @staticmethod
    def syncDependLibsAndEtcFiles(sshTool, nodeName):
        """
        function: Distribute etc file and libsimsearch libs to new node
        input : NA
        output: NA
        """
        try:
            # distribute etc file to new node
            gaussHome = DefaultValue.getEnv("GAUSSHOME")

            searchConfigFile = "%s/etc/searchletConfig.yaml" % gaussHome
            searchIniFile = "%s/etc/searchServer.ini" % gaussHome
            if (os.path.exists(searchConfigFile)):
                sshTool.scpFiles(searchConfigFile, searchConfigFile, nodeName)
            if (os.path.exists(searchIniFile)):
                sshTool.scpFiles(searchIniFile, searchIniFile, nodeName)

            # distribute libsimsearch libs to new node
            libPath = "%s/lib" % gaussHome
            libsimsearchPath = "%s/libsimsearch" % libPath
            if (not os.path.isdir(libsimsearchPath)):
                return

            for node in nodeName:
                cmd = "pscp -H %s '%s' '%s' " % (node, libsimsearchPath,
                                                 libPath)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50214"] % cmd +
                                    " Error: \n%s" % str(output))

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkTransactionReadonly(user, DbclusterInfo, normalCNList=None):
        """
        function : check the CN's parameter default_transaction_read_only is on
                   if eques on, return 1 and error info
        input : user, DbclusterInfo, normalCNList
        output : 0/1
        """
        cnList = []
        if normalCNList is None:
            normalCNList = []
        localhost = DefaultValue.GetHostIpOrName()
        sql = "show default_transaction_read_only;"
        try:
            if (len(normalCNList)):
                cnList = normalCNList
            else:
                # Find CN instance in cluster
                for dbNode in DbclusterInfo.dbNodes:
                    if (len(dbNode.coordinators) != 0):
                        cnList.append(dbNode.coordinators[0])

            nodeInfo = DbclusterInfo.getDbNodeByName(
                DefaultValue.GetHostIpOrName())
            security_mode_value = DefaultValue.getSecurityMode()
            # Execute sql on every CN instance
            if (security_mode_value == "on"):
                for cooInst in cnList:
                    if (localhost == cooInst.hostname):
                        (status, result, error_output) = \
                            ClusterCommand.excuteSqlOnLocalhost(cooInst.port,
                                                                sql)
                        if (status != 2):
                            return 1, "[%s]: Error: %s result: %s status: " \
                                      "%s" % \
                                   (cooInst.hostname, error_output,
                                    result, status)
                        if (result[0][0].strip().lower() == "on"):
                            return 1, "The database is in read only mode."
                    else:
                        currentTime = time.strftime("%Y-%m-%d_%H:%M:%S")
                        pid = os.getpid()
                        outputfile = "metadata_%s_%s_%s.json" % (
                            cooInst.hostname, pid, currentTime)
                        tmpDir = DefaultValue.getTmpDirFromEnv()
                        filepath = os.path.join(tmpDir, outputfile)
                        ClusterCommand.executeSQLOnRemoteHost(cooInst.hostname,
                                                              cooInst.port,
                                                              sql,
                                                              filepath)
                        (status, result, error_output) = \
                            ClusterCommand.getSQLResult(cooInst.hostname,
                                                        outputfile)
                        if (status != 2):
                            return 1, "[%s]: Error: %s result: %s status: " \
                                      "%s" % \
                                   (cooInst.hostname, error_output, result,
                                    status)
                        if (result[0][0].strip().lower() == "on"):
                            return 1, "The database is in read only mode."
            else:
                for cooInst in cnList:
                    (status, output) = ClusterCommand.remoteSQLCommand(
                        sql, user, cooInst.hostname, cooInst.port)
                    resList = output.split('\n')
                    if (status != 0 or len(resList) < 1):
                        return 1, "[%s]: %s" % (cooInst.hostname, output)
                    if (resList[0].strip() == "on"):
                        return 1, "The database is in read only mode."
            return 0, "success"
        except Exception as e:
            return 1, str(e)

    @staticmethod
    def makeCompressedToolPackage(packageDir):
        """
        function : check the output file
        input : String
        output : NA
        """
        # init bin file name, integrity file name and tar list names
        packageDir = os.path.normpath(packageDir)
        bz2FileName = g_OSlib.getBz2FilePath()
        integrityFileName = g_OSlib.getSHA256FilePath()

        tarLists = "--exclude=script/*.log --exclude=*.log script " \
                   "version.cfg lib"
        if "HOST_IP" in os.environ.keys():
            tarLists += " cluster_default_agent.xml"
        try:
            # make compressed tool package
            cmd = "%s && " % g_Platform.getCdCmd(packageDir)
            # do not tar *.log files 
            cmd += g_Platform.getCompressFilesCmd(
                DefaultValue.get_package_back_name(), tarLists)
            cmd += " %s %s " % (os.path.basename(bz2FileName),
                                os.path.basename(integrityFileName))
            cmd += "&& %s " % g_Platform.getChmodCmd(
                str(DefaultValue.KEY_FILE_MODE),
                DefaultValue.get_package_back_name())
            cmd += "&& %s " % g_Platform.getCdCmd("-")
            (status, output) = DefaultValue.retryGetstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s" % output)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def getCpuSet():
        """
        function: get cpu set of current board
                  cat /proc/cpuinfo |grep processor
        input: NA
        output: cpuSet
        """
        # do this function to get the parallel number
        cpuSet = multiprocessing.cpu_count()
        if (cpuSet > 1):
            return cpuSet
        else:
            return DefaultValue.DEFAULT_PARALLEL_NUM

    @staticmethod
    def getTopPathNotExist(topDirPath):
        """
        function : Get the top path if exist
        input : String
        output : String
        """
        tmpDir = topDirPath
        while True:
            # find the top path to be created
            (tmpDir, topDirName) = os.path.split(tmpDir)
            if os.path.exists(tmpDir) or topDirName == "":
                tmpDir = os.path.join(tmpDir, topDirName)
                break
        return tmpDir

    @staticmethod
    def checkSHA256(binFile, sha256File):
        """
        """
        if binFile == "":
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % "bin file")
        if sha256File == "":
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"]
                            % "verification file")

        sha256Obj = hashlib.sha256()
        if not sha256Obj:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50238"] %
                            binFile + "can not get verification Obj.")
        with open(binFile, "rb") as filebin:
            while True:
                strRead = filebin.read(8096)
                if not strRead:
                    break
                sha256Obj.update(strRead)
        strSHA256 = sha256Obj.hexdigest()
        with open(sha256File, "r") as fileSHA256:
            strRead = fileSHA256.readline()
            oldSHA256 = strRead.strip()
            if strSHA256 != oldSHA256:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50238"] % binFile)

    @staticmethod
    def checkDirSize(path, needSize, g_logger):
        """
        function: Check the size of directory
        input : path,needSize
        output: NA
        """
        # The file system of directory
        diskSizeInfo = {}
        dfCmd = "%s | head -2 |tail -1 | %s -F\" \" '{print $1}'" % \
                (g_Platform.getDiskFreeCmd(path), g_Platform.getAwkCmd())
        (status, output) = subprocess.getstatusoutput(dfCmd)
        if (status != 0):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50219"] %
                             "the system file directory" +
                             " Command:%s. Error:\n%s" % (dfCmd, output))

        fileSysName = str(output)
        diskSize = diskSizeInfo.get(fileSysName)
        if (diskSize is None):
            vfs = os.statvfs(path)
            diskSize = vfs.f_bavail * vfs.f_bsize // (1024 * 1024)
            diskSizeInfo[fileSysName] = diskSize

        # 200M for a instance needSize is 200M
        if (diskSize < needSize):
            g_logger.logExit(ErrorCode.GAUSS_504["GAUSS_50400"] % (fileSysName,
                                                                   needSize))

        diskSizeInfo[fileSysName] -= needSize
        return diskSizeInfo

    @staticmethod
    def kill_process(process_name):
        """
        function: kill process
        input : NA
        output: NA
        """
        dfCmd = DefaultValue.killInstProcessCmd(process_name, False, 9, False)
        DefaultValue.execCommandLocally(dfCmd)

    @staticmethod
    def updateRemoteUserEnvVariable(userProfile, variable, value, ssh_tool,
                                    hostnames=None):
        """
        function : Update remote user environment variable
        input : String,String,String
        output : NA
        """
        cmd = "sed -i '\\\/^\\\s*export\\\s*%s=.*$/d' %s;" % (variable,
                                                              userProfile)
        cmd += 'echo \\\"export %s=%s\\\" >> %s' % (variable, value,
                                                    userProfile)
        if hostnames and isinstance(hostnames, list):
            ssh_tool.executeCommand(cmd, "", DefaultValue.SUCCESS, hostnames)
        elif hostnames:
            raise Exception("updateRomoteUserEnvVariable: %s" % (
                    ErrorCode.GAUSS_500["GAUSS_50003"] % (hostnames, "list")))
        else:
            ssh_tool.executeCommand(cmd, "")

    @staticmethod
    def getInstBackupName(inst):
        """
        function : get backup file name (prefix) for the instance
        input    : instance object
        output   : backup file name for this instance
        """
        MAX_BACKUP_FILE_LEN = 128

        backup_name = ''

        if not inst:
            return None
        if not inst.datadir:
            return None

        datadir = inst.datadir
        if len(datadir) < MAX_BACKUP_FILE_LEN:
            backup_name = datadir.lstrip('/').strip('/').replace('/', '_')
        else:
            sha256Obj = hashlib.sha256()
            if not sha256Obj:
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52939"]
                                % "verification Obj.")
            sha256Obj.update(datadir)
            backup_name = sha256Obj.hexdigest()

        return backup_name

    @staticmethod
    def getPrimaryDnNum(dbClusterInfoGucDnPr):
        """
        """
        masterInstance = 0
        dataCount = 0
        dbNodeList = dbClusterInfoGucDnPr.dbNodes
        for dbNode in dbNodeList:
            dataCount = dataCount + dbNode.dataNum
        return dataCount

    @staticmethod
    def getPhysicMemo(PhsshTool, instaLocalMode):
        """
        """
        if instaLocalMode:
            cmd = g_file.SHELL_CMD_DICT["physicMemory"]
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                "Error:\n%s" % str(output))
            else:
                memTotalList = output.split("\n")
                for content in memTotalList:
                    if ("MemTotal" in content):
                        memoList = content.split(":")
                        memo = memoList[1]
                        memo = memo.replace("kB", "")
                        memo = memo.replace("\n", "")
                        memo = memo.strip()
                        memo = int(memo) / 1024 / 1024
            return memo
        physicMemo = []
        cmd = g_file.SHELL_CMD_DICT["physicMemory"]
        (status, output) = PhsshTool.getSshStatusOutput(cmd)
        for ret in status.values():
            if (ret != DefaultValue.SUCCESS):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                "Error:\n%s" % str(output))
        memTotalList = output.split("\n")
        for content in memTotalList:
            if ("MemTotal" in content):
                memoList = content.split(":")
                memo = memoList[1]
                memo = memo.replace("kB", "")
                memo = memo.strip()
                memo = int(memo) / 1024 / 1024
                physicMemo.append(memo)
        minPhysicMemo = min(physicMemo)
        return minPhysicMemo

    @staticmethod
    def getDataNodeNum(dbClusterInfoGucDn):
        """
        """
        masterInstance = 0
        dataNodeNum = []
        dbNodeList = dbClusterInfoGucDn.dbNodes
        for dbNode in dbNodeList:
            dataNodeNum.append(dbNode.dataNum)
        maxDataNodeNum = max(dataNodeNum)
        return maxDataNodeNum

    @staticmethod
    def dynamicGuc(user, logger, instanceType, tmpGucFile, gucXml=False):
        """
        function: set hba config
        input : NA
        output: NA
        """
        try:
            instance = instanceType
            gucList = g_file.readFile(tmpGucFile)
            gucStr = gucList[0].replace("\n", "")
            dynamicParaList = gucStr.split(",")
            for guc in dynamicParaList:
                if (guc == ""):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50203"] %
                                    gucStr)

            # getting the path of guc_list.conf.
            dirName = os.path.dirname(os.path.realpath(__file__))
            if gucXml:
                gucFile = os.path.join(dirName,
                                       "./../etc/conf/guc_cloud_list.xml")
            else:
                gucFile = os.path.join(dirName, "./../etc/conf/guc_list.xml")
            gucFile = os.path.normpath(gucFile)

            # reading xml.
            gucDict = {}
            rootNode = initParserXMLFile(gucFile)
            instanceEle = rootNode.find(instance)
            instanceList = instanceEle.findall("PARAM")
            for gucElement in instanceList:
                DefaultValue.checkGuc(gucElement.attrib['VALUE'], logger)
                gucDict[gucElement.attrib['KEY']] = gucElement.attrib['VALUE']
            gucParaDict = DefaultValue.initGuc(gucDict, logger,
                                               dynamicParaList, gucXml)

            return gucParaDict
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkGuc(gucValue, logger):
        """
        function: check path vaild
        input : envValue
        output: NA
        """
        gucCheckList = ["|", ";", "&", "$", "<", ">", "`", "{", "}", "[", "]",
                        "~", "?", " ", "!"]
        if (gucValue.strip() == ""):
            return
        for rac in gucCheckList:
            flag = gucValue.find(rac)
            if gucValue.strip() == "%x %a %m %u %d %h %p %S" and rac == " ":
                continue
            if flag >= 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % gucValue +
                                " There are illegal characters %s "
                                "in the content." % rac)

    @staticmethod
    def initGuc(gucDict, logger, dynamicParaList, gucXml=False):
        """
        """
        for guc in gucDict:
            if (guc == "comm_max_datanode" and not gucXml):
                if (int(dynamicParaList[0]) < 256):
                    gucDict[guc] = 256
                elif (int(dynamicParaList[0]) < 512):
                    gucDict[guc] = 512
                elif (int(dynamicParaList[0]) < 1024):
                    gucDict[guc] = 1024
                elif (int(dynamicParaList[0]) < 2048):
                    gucDict[guc] = 2048
                else:
                    gucDict[guc] = 4096
                continue
            elif (guc == "max_process_memory"):
                if (gucDict[guc] == "80GB"):
                    continue
                if (int(dynamicParaList[0]) < 256):
                    ratioNum = 1
                elif (int(dynamicParaList[0]) < 512):
                    ratioNum = 2
                else:
                    ratioNum = 3
                gucDict[guc] = gucDict[guc].replace(
                    "PHYSIC_MEMORY", dynamicParaList[1])
                gucDict[guc] = gucDict[guc].replace(
                    "MAX_MASTER_DATANUM_IN_ONENODE", dynamicParaList[2])
                gucDict[guc] = gucDict[guc].replace("N", str(ratioNum))
                try:
                    gucDict[guc] = eval(gucDict[guc])
                except Exception as e:
                    raise Exception(ErrorCode.GAUSS_516["GAUSS_51632"] %
                                    "calculate: %s" % gucDict[guc])
                gucDict[guc] = int(gucDict[guc])
                if (gucDict[guc] >= 2 and gucDict[guc] <= 2047):
                    gucDict[guc] = str(gucDict[guc]) + "GB"
                elif (gucDict[guc] < 2):
                    gucDict[guc] = "2GB"
                else:
                    gucDict[guc] = "2047GB"
                continue
        return gucDict

    @staticmethod
    def getPrivateGucParamList():
        """
        function : Get the private guc parameter list.
        input : NA
        output
        """
        # only used by dummy standby instance
        #     max_connections value is 100
        #     memorypool_enable value is false
        #     shared_buffers value is 32MB
        #     bulk_write_ring_size value is 32MB
        #     max_prepared_transactions value is 10
        #     cstore_buffers value is 16MB
        #     autovacuum_max_workers value is 0
        #     max_pool_size value is 50
        #     wal_buffers value is -1

        # add the parameter content to the dictionary list
        priavetGucParamDict = {}
        priavetGucParamDict["max_connections"] = "100"
        priavetGucParamDict["memorypool_enable"] = "false"
        priavetGucParamDict["shared_buffers"] = "32MB"
        priavetGucParamDict["bulk_write_ring_size"] = "32MB"
        priavetGucParamDict["max_prepared_transactions"] = "10"
        priavetGucParamDict["cstore_buffers"] = "16MB"
        priavetGucParamDict["autovacuum_max_workers"] = "0"
        priavetGucParamDict["wal_buffers"] = "-1"
        priavetGucParamDict["max_locks_per_transaction"] = "64"
        priavetGucParamDict["sysadmin_reserved_connections"] = "3"
        priavetGucParamDict["max_wal_senders"] = "4"
        return priavetGucParamDict

    @staticmethod
    def checkKerberos(mpprcFile):
        """
        function : check kerberos authentication
        input : mpprcfile absolute path
        output : True/False
        """
        krb5Conf = os.path.join(os.path.dirname(mpprcFile),
                                DefaultValue.FI_KRB_CONF)
        tablespace = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
        if (tablespace is not None and tablespace != ""):
            xmlfile = os.path.join(os.path.dirname(mpprcFile),
                                   DefaultValue.FI_ELK_KRB_XML)
        else:
            xmlfile = os.path.join(os.path.dirname(mpprcFile),
                                   DefaultValue.FI_KRB_XML)
        if (os.path.exists(xmlfile) and os.path.exists(krb5Conf) and
                DefaultValue.getEnv("PGKRBSRVNAME")):
            return True
        return False

    @staticmethod
    def get_max_wal_senders_value(max_connections):
        """
        function : Get guc max_wal_senders value by max_connections.
        input : NA
        output
        """
        value = int(max_connections) - 1
        if (value >= DefaultValue.MAX_WAL_SENDERS):
            return DefaultValue.MAX_WAL_SENDERS
        else:
            return value

    @staticmethod
    def setActionFlagFile(module="", logger=None, mode=True):
        """
        function: Set action flag file
        input : module
        output: NAself
        """
        if os.getuid() == 0:
            return
        # Get the temporary directory from PGHOST
        tmpDir = DefaultValue.getTmpDirFromEnv()
        if not tmpDir:
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] % "PGHOST")
        # check if tmp dir exists
        if not os.path.exists(tmpDir):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                            tmpDir + " Please check it.")
        if not os.access(tmpDir, os.R_OK | os.W_OK | os.X_OK):
            raise Exception(ErrorCode.GAUSS_501["GAUSS_50103"] % tmpDir)
        actionFlagFile = os.path.join(tmpDir,
                                      DefaultValue.ACTION_FLAG_FILE + "_%s"
                                      % os.getpid())
        if mode:
            g_file.createFileInSafeMode(actionFlagFile)
            with open(actionFlagFile, "w") as fp:
                fp.write(module)
                fp.flush()
            os.chmod(actionFlagFile, DefaultValue.KEY_FILE_PERMISSION)
        else:
            if os.path.exists(actionFlagFile):
                os.remove(actionFlagFile)

    @staticmethod
    def isUnderUpgrade(user):
        tempPath = DefaultValue.getTmpDirFromEnv(user)
        bakPath = os.path.join(tempPath, "binary_upgrade")
        if os.path.isdir(bakPath):
            if os.listdir(bakPath):
                return True
        return False

    @staticmethod
    def enableWhiteList(sshTool, mpprcFile, nodeNames, logger):
        """
        function: write environment value WHITELIST_ENV for agent mode
        input : sshTool, mpprcFile, nodeNames, logger
        output: NA
        """
        env_dist = os.environ
        if "HOST_IP" in env_dist.keys():
            cmd = "sed -i '/WHITELIST_ENV=/d' %s ; " \
                  "echo 'export WHITELIST_ENV=1' >> %s" % (mpprcFile,
                                                           mpprcFile)
            sshTool.executeCommand(cmd, "Add WHITELIST_ENV",
                                   DefaultValue.SUCCESS, nodeNames)
            logger.debug("Successfully write $WHITELIST_ENV in %s" % mpprcFile)

    @staticmethod
    def disableWhiteList(sshTool, mpprcFile, nodeNames, logger):
        """
        function: delete environment value WHITELIST_ENV for agent mode
        input : NA
        output: NA
        """
        env_dist = os.environ
        if "HOST_IP" in env_dist.keys():
            cmd = "sed -i '/WHITELIST_ENV=/d' %s && unset WHITELIST_ENV" % \
                  mpprcFile
            sshTool.executeCommand(cmd, "Clear WHITELIST_ENV",
                                   DefaultValue.SUCCESS, nodeNames)
            logger.debug(
                "Successfully clear $WHITELIST_ENV in %s." % mpprcFile)

    @staticmethod
    def checkDockerEnv():
        cmd = "egrep  '^1:.+(docker|lxc|kubepods)' /proc/1/cgroup"
        (status, output) = subprocess.getstatusoutput(cmd)
        if output:
            return True
        else:
            return False


class ClusterCommand():
    '''
    Common for cluster command
    '''

    def __init__(self):
        pass

    # gs_sshexkey execution takes total steps
    TOTAL_STEPS_SSHEXKEY = 11
    # gs_preinstall -L execution takes total steps
    TOTAL_STEPS_PREINSTALL_L = 14
    # gs_preinstall execution takes total steps
    TOTAL_STEPS_PREINSTALL = 17
    # gs_install execution takes total steps
    TOTAL_STEPS_INSTALL = 7
    # gs_om -t managecn -m add execution takes total steps
    TOTAL_STEPS_OM_ADD = 20
    # gs_om -t managecn -m delete execution takes total steps
    TOTAL_STEPS_OM_DELETE = 16
    # gs_om -t changeip execution takes total steps
    TOTAL_STEPS_OM_CHANGEIP = 11
    # gs_expand -t dilatation execution takes total steps
    TOTAL_STEPS_EXPAND_DILA = 17
    # gs_expand -t redistribute execution takes total steps
    TOTAL_STEPS_EXPAND_REDIS = 6
    # gs_shrink -t entry1_percontraction execution takes total steps
    TOTAL_STEPS_SHRINK_FIRST = 9
    # gs_shrink -t entry2_redistributre execution takes total steps
    TOTAL_STEPS_SHRINK_SECOND = 8
    # gs_shrink -t entry3_postcontraction execution takes total steps
    TOTAL_STEPS_SHRINK_THIRD = 7
    # gs_replace -t warm-standby execution takes total steps
    TOTAL_STEPS_REPLACE_WARM_STANDBY = 11
    # gs_replace -t warm-standby rollback replace execution takes total steps
    TOTAL_STEPS_REPLACE_WARM_STANDBY_REPLACE = 9
    # gs_replace -t warm-standby rollback install execution takes total steps
    TOTAL_STEPS_REPLACE_WARM_STANDBY_INSTALL = 7
    # gs_replace -t warm-standby rollback config execution takes total steps
    TOTAL_STEPS_REPLACE_WARM_STANDBY_CONFIG = 6
    # gs_replace -t install execution takes total steps
    TOTAL_STEPS_REPLACE_INSTALL = 6
    # gs_replace -t config execution takes total steps
    TOTAL_STEPS_REPLACE_CONFIG = 6
    # gs_replace -t start execution takes total steps
    TOTAL_STEPS_REPLACE_START = 3
    # gs_uninstall execution takes total steps
    TOTAL_STEPS_UNINSTALL = 8
    # gs_upgradectl -t auto-upgrade execution takes total steps
    TOTAL_STEPS_GREY_UPGRADECTL = 12
    # gs_upgradectl -t auto-upgrade --inplace execution takes total steps
    TOTAL_STEPS_INPLACE_UPGRADECTL = 15
    # gs_postuninstall execution takes total steps
    TOTAL_STEPS_POSTUNINSTALL = 3
    # warm-standby rollback to flag of begin warm standby
    WARM_STEP_INIT = "Begin warm standby"
    # warm-standby rollback to flag of replace IP finished
    WARM_STEP_REPLACEIPS = "Replace IP finished"
    # warm-standby rollback to flag of install warm standby nodes finished
    WARM_STEP_INSTALL = "Install warm standby nodes finished"
    # warm-standby rollback to flag of configure warm standby nodes finished
    WARM_STEP_CONFIG = "Configure warm standby nodes finished"
    # rollback to flag of start cluster
    INSTALL_STEP_CONFIG = "Config cluster"
    # rollback to flag of start cluster
    INSTALL_STEP_START = "Start cluster"

    @staticmethod
    def getRedisCmd(user, port, jobs=1, timeout=None, enableVacuum="",
                    enableFast="", redisRetry="", buildTable=False,
                    mode="", host="", database="postgres"):
        """
        funciton : Get the command of gs_redis with password for redisuser
        input : user: data redis_user
                port: the port redis_user connect to server
                jobs: data redis parallel nums
                enableVacuum: is need vacuum
                enableFast: doing fast data redistribution or not
                redisRetry: retry to excute data redis
                buildTable: create pgxc_redistb or not
                mode: insert or read-only mode
                database: database which need to data redis
        output : String
        """
        userProfile = DefaultValue.getMpprcFile()
        database = database.replace('$', '\$')
        cmd = "%s %s ; gs_redis -u %s -p %s -d %s -j %d %s %s %s" % \
              (g_Platform.getSourceCmd(), userProfile, user, str(port),
               database, jobs, enableVacuum, enableFast, redisRetry)
        # check timeout
        if (timeout is not None):
            cmd += " -t %d" % timeout
        # check buildTable
        if buildTable:
            cmd += " -v"
        else:
            cmd += " -r"
        # check mode
        if (len(mode)):
            cmd += " -m %s" % mode

        return cmd

    @staticmethod
    def getQueryStatusCmd(user, hostName="", outFile="", showAll=True):
        """
        function : Get the command of querying status of cluster or node
        input : String
        output : String
        """
        userProfile = DefaultValue.getMpprcFile()
        cmd = "%s %s ; gs_om -t status" % (g_Platform.getSourceCmd(),
                                           userProfile)
        # check node id
        if (hostName != ""):
            cmd += " -h %s" % hostName
        else:
            if (showAll):
                cmd += " --all"
        # check out put file
        if (outFile != ""):
            cmd += " > %s" % outFile

        return cmd

    @staticmethod
    def findErrorInSqlFile(sqlFile, output):
        """
        function : Find error in the sql file
        input : String,String
        output : String
        """
        GSQL_BIN_FILE = "gsql"
        # init flag
        ERROR_MSG_FLAG = "(ERROR|FATAL|PANIC)"
        GSQL_ERROR_PATTERN = "^%s:%s:(\d*): %s:.*" % \
                             (GSQL_BIN_FILE, sqlFile, ERROR_MSG_FLAG)
        pattern = re.compile(GSQL_ERROR_PATTERN)
        for line in output.split("\n"):
            line = line.strip()
            result = pattern.match(line)
            if (result is not None):
                return True
        return False

    @staticmethod
    def findErrorInSql(output):
        """
        function : Find error in sql
        input : String
        output : boolean 
        """
        # init flag
        ERROR_MSG_FLAG = "(ERROR|FATAL|PANIC)"
        ERROR_PATTERN = "^%s:.*" % ERROR_MSG_FLAG
        pattern = re.compile(ERROR_PATTERN)

        for line in output.split("\n"):
            line = line.strip()
            result = pattern.match(line)
            if (result is not None):
                return True
        return False

    @staticmethod
    def getSQLCommand(port, database=DefaultValue.DEFAULT_DB_NAME,
                      gsqlBin="gsql", host=""):
        """
        function : get SQL command
        input : port, database
        output : cmd
        """
        cmd = DefaultValue.SQL_EXEC_COMMAND_WITHOUT_HOST_WITHOUT_USER % \
              (gsqlBin, str(int(port) + 1), database)
        return cmd

    @staticmethod
    def getSQLCommandForInplaceUpgradeBackup(
            port, database=DefaultValue.DEFAULT_DB_NAME, gsqlBin="gsql"):
        """
        function: get SQL command for Inplace
                  Upgrade backupOneInstanceOldClusterDBAndRel
        input: port, database
        output: cmd
        """
        cmd = DefaultValue.SQL_EXEC_COMMAND_WITHOUT_HOST_WITHOUT_USER % (
            gsqlBin, port, database)
        return cmd

    @staticmethod
    def execSQLCommand(sql, user, host, port, database="postgres",
                       dwsFlag=False, option="", IsInplaceUpgrade=False):
        """
        function : Execute sql command
        input : String,String,String,int
        output : String
        """
        database = database.replace('$', '\$')
        currentTime = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S%f")
        pid = os.getpid()
        # init SQL query file
        sqlFile = os.path.join(
            DefaultValue.getTmpDirFromEnv(user),
            "gaussdb_query.sql_%s_%s_%s" % (str(port), str(currentTime),
                                            str(pid)))
        # init SQL result file
        queryResultFile = os.path.join(
            DefaultValue.getTmpDirFromEnv(user),
            "gaussdb_result.sql_%s_%s_%s" % (str(port), str(currentTime),
                                             str(pid)))
        if os.path.exists(sqlFile) or os.path.exists(queryResultFile):
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
        # create an empty sql query file
        try:
            g_file.createFile(sqlFile, DefaultValue.KEY_FILE_MODE)
        except Exception as e:
            if os.path.exists(sqlFile):
                os.remove(sqlFile)
            return 1, str(e)

        # witer the SQL command into sql query file
        try:
            g_file.createFileInSafeMode(sqlFile)
            with open(sqlFile, 'w') as fp:
                fp.writelines(sql)
        except Exception as e:
            DefaultValue.cleanFile(sqlFile)
            return 1, str(e)
        try:
            # init hostPara
            userProfile = DefaultValue.getMpprcFile()
            hostPara = ("-h %s" % host) if host != "" else ""
            # build shell command
            # if the user is root, switch the user to execute
            if (IsInplaceUpgrade):
                gsqlCmd = ClusterCommand.getSQLCommandForInplaceUpgradeBackup(
                    port, database)
            else:
                gsqlCmd = ClusterCommand.getSQLCommand(
                    port, database, host=host)
            executeCmd = "%s %s -f '%s' --output '%s' -t -A -X %s" % (
                gsqlCmd, hostPara, sqlFile, queryResultFile, option)
            cmd = g_Platform.getExecuteCmdWithUserProfile(user, userProfile,
                                                          executeCmd, False)
            (status, output) = subprocess.getstatusoutput(cmd)
            if ClusterCommand.findErrorInSqlFile(sqlFile, output):
                status = 1
            if (status != 0):
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
                return (status, output)
            # read the content of query result file.
        except Exception as e:
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
            raise Exception(str(e))
        try:
            with open(queryResultFile, 'r') as fp:
                rowList = fp.readlines()
        except Exception as e:
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
            return 1, str(e)

        # remove local sqlFile
        DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))

        return (0, "".join(rowList)[:-1])

    @staticmethod
    def findTupleErrorInSqlFile(sqlFile, output):
        """
        function : find tuple concurrently updated error in file
        input : sqlFile, output
        output : True, False
        """
        ERROR_TUPLE_PATTERN = "^gsql:(.*)tuple concurrently updated(.*)"
        pattern = re.compile(ERROR_TUPLE_PATTERN)
        for line in output.split("\n"):
            line = line.strip()
            result = pattern.match(line)
            if (result is not None):
                return True
        return False

    @staticmethod
    def remoteSQLCommand(sql, user, host, port, ignoreError=True,
                         database="postgres", dwsFlag=False, useTid=False,
                         IsInplaceUpgrade=False):
        """
        function : Execute sql command on remote host
        input : String,String,String,int
        output : String,String
        """
        database = database.replace('$', '\$')
        currentTime = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S%f")
        pid = os.getpid()
        # clean old sql file
        # init SQL query file
        sqlFile = os.path.join(DefaultValue.getTmpDirFromEnv(user),
                               "gaussdb_remote_query.sql_%s_%s_%s" % (
                                   str(port),
                                   str(currentTime),
                                   str(pid)))
        # init SQL result file
        queryResultFile = os.path.join(DefaultValue.getTmpDirFromEnv(user),
                                       "gaussdb_remote_result.sql_%s_%s_%s" % (
                                           str(port),
                                           str(currentTime),
                                           str(pid)))
        RE_TIMES = 3
        if useTid:
            threadPid = CDLL('libc.so.6').syscall(186)
            sqlFile = sqlFile + str(threadPid)
            queryResultFile = queryResultFile + str(threadPid)
        if (os.path.exists(sqlFile) or os.path.exists(queryResultFile)):
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))

        # create new sql file
        if (os.getuid() == 0):
            cmd = "su - %s -c 'touch %s && chmod %s %s'" % (
                user, sqlFile, DefaultValue.KEY_FILE_MODE, sqlFile)
        else:
            cmd = "touch %s && chmod %s %s" % (sqlFile,
                                               DefaultValue.KEY_FILE_MODE,
                                               sqlFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            output = "%s\n%s" % (cmd, output)
            if (os.path.exists(sqlFile) or os.path.exists(queryResultFile)):
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
            return (status, output)

        # witer the SQL command into sql query file
        try:
            g_file.createFileInSafeMode(sqlFile)
            with open(sqlFile, 'w') as fp:
                fp.writelines(sql)
        except Exception as e:
            DefaultValue.cleanFile(sqlFile)
            return (1, str(e))

        # send new sql file to remote node if needed
        localHost = DefaultValue.GetHostIpOrName()
        if str(localHost) != str(host):
            cmd = g_Platform.getRemoteCopyCmd(sqlFile, sqlFile, host)
            if os.getuid() == 0 and user != "":
                cmd = "su - %s \"%s\"" % (user, cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
                output = "%s\n%s" % (cmd, output)
                return (status, output)

        # execute sql file
        mpprcFile = DefaultValue.getMpprcFile()
        if IsInplaceUpgrade:
            gsql_cmd = ClusterCommand.getSQLCommandForInplaceUpgradeBackup(
                port, database)
        else:
            gsql_cmd = ClusterCommand.getSQLCommand(port, database, host=host)

        if str(localHost) != str(host):
            sshCmd = g_Platform.getSshCmd(host)
            if os.getuid() == 0 and user != "":
                cmd = " %s 'su - %s -c \"" % (sshCmd, user)
                if mpprcFile != "" and mpprcFile is not None:
                    cmd += "source %s;" % mpprcFile
                cmd += "%s -f %s --output %s -t -A -X \"'" % (gsql_cmd,
                                                              sqlFile,
                                                              queryResultFile)
                if ignoreError:
                    cmd += " 2>/dev/null"
            else:
                cmd = "%s '" % sshCmd
                if mpprcFile != "" and mpprcFile is not None:
                    cmd += "source %s;" % mpprcFile
                cmd += "%s -f %s --output %s -t -A -X '" % (gsql_cmd,
                                                            sqlFile,
                                                            queryResultFile)
                if ignoreError:
                    cmd += " 2>/dev/null"
            for i in range(RE_TIMES):
                (status1, output1) = subprocess.getstatusoutput(cmd)
                if ClusterCommand.findErrorInSqlFile(sqlFile, output1):
                    if (ClusterCommand.findTupleErrorInSqlFile(sqlFile,
                                                               output1)):
                        time.sleep(1)  # find tuple error --> retry
                    else:  # find error not tuple error
                        status1 = 1
                        break
                else:  # not find error
                    break
            # if failed to execute gsql, then clean the sql query file on
            # current node and other node
            if (status1 != 0):
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile),
                                       host)
                return (status1, output1)
        else:
            if (os.getuid() == 0 and user != ""):
                cmd = "su - %s -c \"" % user
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "%s -f %s --output %s -t -A -X \"" % (gsql_cmd,
                                                             sqlFile,
                                                             queryResultFile)
                if (ignoreError):
                    cmd += " 2>/dev/null"
            else:
                cmd = ""
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "%s -f %s --output %s -t -A -X " % (gsql_cmd,
                                                           sqlFile,
                                                           queryResultFile)
                if (ignoreError):
                    cmd += " 2>/dev/null"
            for i in range(RE_TIMES):
                (status1, output1) = subprocess.getstatusoutput(cmd)
                if ClusterCommand.findErrorInSqlFile(sqlFile, output1):
                    if (ClusterCommand.findTupleErrorInSqlFile(sqlFile,
                                                               output1)):
                        time.sleep(1)  # find tuple error --> retry
                    else:  # find error not tuple error
                        status1 = 1
                        break
                else:  # not find error
                    break
            # if failed to execute gsql, then clean the sql query file
            # on current node and other node
            if (status1 != 0):
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
                return (status1, output1)

        if (str(localHost) != str(host)):
            remoteCmd = g_Platform.getRemoteCopyCmd(
                queryResultFile,
                DefaultValue.getTmpDirFromEnv(user) + "/", str(localHost))
            cmd = "%s \"%s\"" % (sshCmd, remoteCmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                output = "%s\n%s" % (cmd, output)
                DefaultValue.cleanFile(sqlFile)
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile),
                                       host)
                return (status, output)

        # read the content of query result file.
        try:
            with open(queryResultFile, 'r') as fp:
                rowList = fp.readlines()
        except Exception as e:
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
            if (str(localHost) != str(host)):
                DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile),
                                       host)
            return (1, str(e))

        # remove local sqlFile
        DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile))
        # remove remote sqlFile
        if (str(localHost) != str(host)):
            DefaultValue.cleanFile("%s,%s" % (queryResultFile, sqlFile), host)

        return (0, "".join(rowList)[:-1])

    @staticmethod
    def checkSqlConnect(user, host, port,
                        retryTimes=DefaultValue.DEFAULT_RETRY_TIMES_GS_CTL,
                        sql=None, dwsFlag=False):
        """
        After the operation "gs_ctl start" has returned the success
         information, we will try to connect the database
         and execute some sql to check the connection.

        :param user:        The input database user.
        :param host:        The input database host or ip address.
        :param port:        The input database port.
        :param retryTimes:  The times of attempts to retry the operation.
        :param sql:         The SQL statements used in retry operation.
        :param dwsFlag:     Whether the cluster is in the dws mode.

        :type user:         str
        :type host:         str
        :type port:         int
        :type retryTimes:   int
        :type sql:          str | None
        :type dwsFlag:      bool

        :return:    Return the query result.
        :rtype:     str
        """
        # Set default query sql string.
        if sql is None:
            sql = "select version();"

        for i in range(0, retryTimes):
            status, output = ClusterCommand.remoteSQLCommand(sql, user, host,
                                                             port, False,
                                                             dwsFlag=dwsFlag)
            if status == 0 and output != "":
                return output

            time.sleep(2)

        raise Exception(ErrorCode.GAUSS_516["GAUSS_51632"] %
                        "check instance connection.")

    @staticmethod
    def remoteShellCommand(shell, user, hostname):
        """
        function : Execute shell command on remote host
        input : String,String,String
        output : String,String
        """
        currentTime = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S%f")
        randomnum = ''.join(sample('0123456789', 3))
        pid = os.getpid()
        shFile = os.path.join(DefaultValue.getTmpDirFromEnv(user),
                              "gaussdb_remote_shell.sh_%s_%s_%s_%s" % \
                              (str(hostname), str(currentTime), str(pid),
                               str(randomnum)))
        if (os.path.exists(shFile)):
            DefaultValue.cleanFile(shFile)

        # create new sh file
        if (os.getuid() == 0):
            cmd = "su - %s -c 'touch %s && chmod %s %s'" % \
                  (user, shFile, DefaultValue.KEY_FILE_MODE, shFile)
        else:
            cmd = "touch %s && chmod %s %s" % \
                  (shFile, DefaultValue.KEY_FILE_MODE, shFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            if (os.path.exists(shFile)):
                DefaultValue.cleanFile(shFile)
            output = "%s\n%s" % (cmd, output)
            return (status, output)

        try:
            with open(shFile, 'w') as fp:
                fp.writelines(shell)
        except Exception as e:
            if (fp):
                fp.close()
            DefaultValue.cleanFile(shFile)
            return (1, str(e))

        # send new sh file to remote node if needed
        localHost = DefaultValue.GetHostIpOrName()
        if (str(localHost) != str(hostname)):
            if (os.getuid() == 0):
                cmd = """su - %s -c "pscp -H %s '%s' '%s'" """ % \
                      (user, hostname, shFile, shFile)
            else:
                cmd = "pscp -H %s '%s' '%s'" % (hostname, shFile, shFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                DefaultValue.cleanFile(shFile)
                output = "%s\n%s" % (cmd, output)
                return (status, output)

        # execute sh file
        if (str(localHost) != str(hostname)):
            mpprcFile = DefaultValue.getMpprcFile()
            if (os.getuid() == 0):
                cmd = "pssh -s -H %s  'su - %s -c \"" % \
                      (hostname, user)
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "sh %s\"'" % shFile
            else:
                cmd = "pssh -s -H %s '" % hostname
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "sh %s'" % shFile
        else:
            mpprcFile = DefaultValue.getMpprcFile()
            if (os.getuid() == 0):
                cmd = "su - %s -c '" % user
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "sh %s'" % shFile
            else:
                cmd = ""
                if (mpprcFile != "" and mpprcFile is not None):
                    cmd += "source %s;" % mpprcFile
                cmd += "sh %s" % shFile

        (status, output) = subprocess.getstatusoutput(cmd)

        # clean tmp file
        DefaultValue.cleanFile(shFile)
        if (str(localHost) != str(hostname)):
            DefaultValue.cleanFile(shFile, hostname)

        return (status, output)

    @staticmethod
    def CopyClusterStatic():
        """
        function : Copy cluster_static_config_bak file to cluster_static_config
        input : NA
        output: NA
        """
        gaussHome = DefaultValue.getEnv("GAUSSHOME")
        staticConfig = "%s/bin/cluster_static_config" % gaussHome
        staticConfig_bak = "%s/bin/cluster_static_config_bak" % gaussHome
        if (os.path.exists(staticConfig_bak) and not
        os.path.exists(staticConfig)):
            g_file.cpFile(staticConfig_bak, staticConfig)

    @staticmethod
    def getchangeDirModeCmd(user_dir):
        """
        function : change directory permission
        input : user_dir
        output: NA
        """
        # Use "find -exec" to mask special characters
        cmdDir = "find '%s' -type d -exec chmod '%s' {} \;" % \
                 (user_dir, DefaultValue.KEY_DIRECTORY_MODE)
        (status, diroutput) = subprocess.getstatusoutput(cmdDir)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % user_dir +
                            " Command:%s. Error:\n%s" % (cmdDir, diroutput))

    @staticmethod
    def getchangeFileModeCmd(user_dir):
        """
        function : change log file permission
        input : user_dir
        output: NA
        """
        # Use "find -exec" to mask special characters
        cmdFile = "find '%s' -type f -name '*.log' -exec chmod '%s' {} \;" % \
                  (user_dir, DefaultValue.KEY_FILE_MODE)
        (status, fileoutput) = subprocess.getstatusoutput(cmdFile)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                            "log file" + " Directory:%s." % user_dir +
                            " Command:%s. Error:\n%s" % (cmdFile, fileoutput))

    @staticmethod
    def countTotalSteps(script, act="", model=""):
        """
        function: get script takes steps in total
        input:
            script: command name
            act: the type of command
            model: mode setting
        """
        try:
            totalSteps = 0
            if (script == "gs_preinstall"):
                if model:
                    totalSteps = ClusterCommand.TOTAL_STEPS_PREINSTALL_L
                else:
                    totalSteps = ClusterCommand.TOTAL_STEPS_PREINSTALL
            elif (script == "gs_install"):
                if (model == ClusterCommand.INSTALL_STEP_CONFIG):
                    totalSteps = ClusterCommand.TOTAL_STEPS_INSTALL - 1
                elif (model == ClusterCommand.INSTALL_STEP_START):
                    totalSteps = ClusterCommand.TOTAL_STEPS_INSTALL - 2
                else:
                    totalSteps = ClusterCommand.TOTAL_STEPS_INSTALL
            elif (script == "gs_om"):
                if (act == "managecn"):
                    if (model == "add"):
                        totalSteps = ClusterCommand.TOTAL_STEPS_OM_ADD
                    if (model == "delete"):
                        totalSteps = ClusterCommand.TOTAL_STEPS_OM_DELETE
                if (act == "changeip"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_OM_CHANGEIP
            elif (script == "gs_expand"):
                if (act == "dilatation"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_EXPAND_DILA
                if (act == "redistribute"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_EXPAND_REDIS
            elif (script == "gs_shrink"):
                if (act == "entry1"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_SHRINK_FIRST
                if (act == "entry2"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_SHRINK_SECOND
                if (act == "entry3"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_SHRINK_THIRD
            elif (script == "gs_sshexkey"):
                if model:
                    totalSteps = ClusterCommand.TOTAL_STEPS_SSHEXKEY - 2
                else:
                    totalSteps = ClusterCommand.TOTAL_STEPS_SSHEXKEY
            elif (script == "gs_replace"):
                if (act == "warm-standby"):
                    if (model == ClusterCommand.WARM_STEP_INIT):
                        totalSteps = ClusterCommand. \
                            TOTAL_STEPS_REPLACE_WARM_STANDBY
                    if (model == ClusterCommand.WARM_STEP_REPLACEIPS):
                        totalSteps = ClusterCommand. \
                            TOTAL_STEPS_REPLACE_WARM_STANDBY_REPLACE
                    if (model == ClusterCommand.WARM_STEP_INSTALL):
                        totalSteps = ClusterCommand. \
                            TOTAL_STEPS_REPLACE_WARM_STANDBY_INSTALL
                    if (model == ClusterCommand.WARM_STEP_CONFIG):
                        totalSteps = ClusterCommand. \
                            TOTAL_STEPS_REPLACE_WARM_STANDBY_CONFIG
                if (act == "install"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_REPLACE_INSTALL
                if (act == "config"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_REPLACE_CONFIG
                if (act == "start"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_REPLACE_START
            elif (script == "gs_upgradectl"):
                if (act == "small-binary-upgrade" or act ==
                        "large-binary-upgrade"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_GREY_UPGRADECTL
                if (act == "inplace-binary-upgrade"):
                    totalSteps = ClusterCommand.TOTAL_STEPS_INPLACE_UPGRADECTL
            elif (script == "gs_uninstall"):
                totalSteps = ClusterCommand.TOTAL_STEPS_UNINSTALL
            elif (script == "gs_postuninstall"):
                totalSteps = ClusterCommand.TOTAL_STEPS_POSTUNINSTALL
            return totalSteps
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def check_input(jsonFilePath):
        """
        function: check the input, and load the backup JSON file.
        @param: N/A.
        @return: return [OK, para], if the backup JSON file is loaded
                successfully.
        """
        try:
            with open(jsonFilePath) as jsonFile:
                para = json.load(jsonFile)
            return [0, para]
        except TypeError as err:
            ERR_MSG = "input para is not json_string. %s" % err
            return [1, ERR_MSG]

    @staticmethod
    def executeSQLOnRemoteHost(hostName, port, sql, outputfile,
                               snapid="defaultNone", database="postgres"):
        """
        function: execute SQL on remote host
        input :hostName, port, sql, outputfile, database
        output: NA
        """
        from gspylib.threads.SshTool import SshTool
        from gspylib.common.OMCommand import OMCommand
        hosts = []
        hosts.append(hostName)
        gs_sshTool = SshTool(hosts)
        currentTime = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S%f")
        pid = os.getpid()
        sqlfile = "%s_%s_%s.sql" % (hostName, pid, currentTime)
        tmpDir = DefaultValue.getTmpDirFromEnv() + "/"
        sqlfilepath = os.path.join(tmpDir, sqlfile)
        g_file.createFileInSafeMode(sqlfilepath)
        try:
            with open(sqlfilepath, "w") as fp:
                fp.write(sql)
                fp.flush()

            g_OSlib.scpFile(hostName, sqlfilepath, tmpDir)
            cmd = "%s  -p %s -S %s -f %s -s %s -d %s" % (
                OMCommand.getLocalScript("Local_Execute_Sql"), port,
                sqlfilepath, outputfile, snapid, database)
            gs_sshTool.executeCommand(cmd, "execute SQL on remote host")
            cmd = "%s %s" % (g_Platform.getRemoveCmd("directory"), sqlfilepath)
            (status, output) = subprocess.getstatusoutput(cmd)
        except Exception as e:
            cmd = "%s %s" % (g_Platform.getRemoveCmd("directory"), sqlfilepath)
            (status, output) = subprocess.getstatusoutput(cmd)
            raise Exception(str(e))

    @staticmethod
    def excuteSqlOnLocalhost(port, sql, database="postgres"):
        '''
        function: write output message
        input : sql
        output: NA
        '''
        tmpresult = None
        conn = None
        try:
            from gspylib.common.SqlResult import sqlResult
            libpath = os.path.join(DefaultValue.getEnv("GAUSSHOME"), "lib")
            sys.path.append(libpath)
            libc = cdll.LoadLibrary("libpq.so.5.5")
            conn_opts = "dbname = '%s' application_name = 'OM' " \
                        "options='-c xc_maintenance_mode=on'  port = %s " % \
                        (database, port)
            conn_opts = conn_opts.encode(encoding='utf-8')
            err_output = ""
            libc.PQconnectdb.argtypes = [c_char_p]
            libc.PQconnectdb.restype = c_void_p
            libc.PQclear.argtypes = [c_void_p]
            libc.PQfinish.argtypes = [c_void_p]
            libc.PQerrorMessage.argtypes = [c_void_p]
            libc.PQerrorMessage.restype = c_char_p
            libc.PQresultStatus.argtypes = [c_void_p]
            libc.PQresultStatus.restype = c_int
            libc.PQexec.argtypes = [c_void_p, c_char_p]
            libc.PQexec.restype = c_void_p
            conn = libc.PQconnectdb(conn_opts)
            if not conn:
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51310"]
                                % ("by options: %s." % conn_opts))
            sql = sql.encode(encoding='utf-8')
            libc.PQstatus.argtypes = [c_void_p]
            if (libc.PQstatus(conn) != 0):
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51310"] % ".")
            tmpresult = libc.PQexec(conn, sql)
            if not tmpresult:
                raise Exception(ErrorCode.GAUSS_513["GAUSS_51309"] % sql)
            status = libc.PQresultStatus(tmpresult)

            resultObj = sqlResult(tmpresult)
            resultObj.parseResult()
            Error = libc.PQerrorMessage(conn)
            if (Error is not None):
                err_output = string_at(Error).decode()
            result = resultObj.resSet
            libc.PQclear(tmpresult)
            libc.PQfinish(conn)
            return status, result, err_output
        except Exception as e:
            libc.PQclear.argtypes = [c_void_p]
            libc.PQfinish.argtypes = [c_void_p]
            if tmpresult:
                libc.PQclear(tmpresult)
            if conn:
                libc.PQfinish(conn)
            raise Exception(str(e))

    @staticmethod
    def getSQLResult(hostName, jsonFile):
        """
        function: get sql result from jsonFile
        input : hostName,jsonFile
        output: status, result, error_output
        """
        # copy json file from remote host
        tmpDir = DefaultValue.getTmpDirFromEnv() + "/"
        filepath = os.path.join(tmpDir, jsonFile)
        scpCmd = g_Platform.getRemoteCopyCmd(filepath, tmpDir, hostName,
                                             False, "directory")
        DefaultValue.execCommandLocally(scpCmd)
        # parse json file
        status = ""
        result = []
        error_output = ""
        (ret, para) = ClusterCommand.check_input(filepath)
        if (ret != 0):
            raise Exception(ErrorCode.GAUSS_513["GAUSS_51308"])

        if "status" not in para:
            raise Exception(ErrorCode.GAUSS_513["GAUSS_51307"])
        else:
            status = para["status"]

        if "result" not in para:
            raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"] % "")
        else:
            result = para["result"]
        if "error_output" in para:
            error_output = para["error_output"]

            # remove json file from remote host and localhost
        g_file.removeDirectory(filepath)

        remoteCmd = g_Platform.getSshCmd(hostName)
        cmd = "%s \"%s '%s'\"" % (remoteCmd,
                                  g_Platform.getRemoveCmd("directory"),
                                  filepath)
        DefaultValue.execCommandLocally(cmd)

        return status, result, error_output

    @staticmethod
    def checkInstStatusByGsctl(instdir, retryCount=100):
        """
        function: check single instance status for local instance.
                Wait for 5 minutes. If the instance status is still Catchup,
                the instance status is Normal.
        input: NA
        output: (status, output)
        """
        count = 0
        while (count < retryCount):
            time.sleep(3)
            count += 1
            cmd = "gs_ctl query -D %s|grep '\<db_state\>'| " \
                  "awk -F ':' '{print $2}'" % instdir
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0 and output.strip() == "Normal"):
                break
            elif (status == 0 and count == retryCount and output.strip() ==
                  "Catchup"):
                output = "Normal"
        return (status, output)


class ClusterInstanceConfig():
    """
    Set Instance Config
    """

    def __init__(self):
        pass

    @staticmethod
    def setConfigItem(typename, datadir, configFile, parmeterDict):
        """
        function: Modify a parameter
        input : typename, datadir, configFile, parmeterDict
        output: NA
        """
        # check mpprc file path
        mpprcFile = DefaultValue.getMpprcFile()

        # comment out any existing entries for this setting
        if (typename == DefaultValue.INSTANCE_ROLE_CMSERVER or typename ==
                DefaultValue.INSTANCE_ROLE_CMAGENT):
            # gs_guc only support for DB instance
            # if the type is cm_server or cm_agent, we will use sed to
            # instead of it
            for entry in parmeterDict.items():
                key = entry[0]
                value = entry[1]
                # delete the old parameter information
                cmd = "sed -i 's/^.*\(%s.*=.*\)/#\\1/g' %s" % (key, configFile)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50008"] +
                                    " Command:%s. Error:\n%s" % (cmd, output))

                # append new config to file
                cmd = 'echo "      " >> %s' % (configFile)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                    " Error: \n%s" % output)

                cmd = 'echo "%s = %s" >> %s' % (key, value, configFile)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                    " Error: \n%s" % output)
        else:
            # build GUC parameter string
            gucstr = ""
            for entry in parmeterDict.items():
                gucstr += " -c \"%s=%s\"" % (entry[0], entry[1])
            # check the GUC parameter string
            if (gucstr == ""):
                return
            cmd = "source %s; gs_guc set -D %s %s" % \
                  (mpprcFile, datadir, gucstr)
            DefaultValue.retry_gs_guc(cmd)

    @staticmethod
    def setReplConninfo(dbInst, peerInsts, clusterInfo):
        """
        function: Modify replconninfo for datanode
        input : dbInst
        output: NA
        """
        masterInst = None
        standbyInst = None
        dummyStandbyInst = None
        nodename = ""
        # init masterInst, standbyInst and dummyStandbyInst
        for pi in iter(peerInsts):
            if (pi.instanceType == DefaultValue.MASTER_INSTANCE):
                masterInst = pi
            elif (pi.instanceType == DefaultValue.STANDBY_INSTANCE):
                standbyInst = pi
            elif (pi.instanceType ==
                  DefaultValue.DUMMY_STANDBY_INSTANCE):
                dummyStandbyInst = pi

        if (dbInst.instanceType == DefaultValue.MASTER_INSTANCE):
            masterInst = dbInst
            nodename = "dn_%d_%d" % (masterInst.instanceId,
                                     standbyInst.instanceId)
        elif (dbInst.instanceType == DefaultValue.STANDBY_INSTANCE):
            standbyInst = dbInst
            nodename = "dn_%d_%d" % (masterInst.instanceId,
                                     standbyInst.instanceId)
        elif (dbInst.instanceType == DefaultValue.DUMMY_STANDBY_INSTANCE):
            dummyStandbyInst = dbInst
            nodename = "dn_%d_%d" % (masterInst.instanceId,
                                     dummyStandbyInst.instanceId)
        if (len(masterInst.haIps) == 0 or len(standbyInst.haIps) == 0):
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51621"] +
                            " Data directory: %s." % dbInst.datadir)
        if (dummyStandbyInst is not None and len(dummyStandbyInst.haIps) == 0):
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51621"] +
                            " Data directory: %s." % dbInst.datadir)

        connInfo1 = ""
        connInfo2 = ""
        channelCount = len(masterInst.haIps)
        # get master instance number
        masterDbNode = clusterInfo.getDbNodeByName(masterInst.hostname)
        if masterDbNode is None:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                            ("database node configuration on host [%s]"
                             % masterInst.hostname))
        masterDataNum = masterDbNode.getDnNum(masterInst.instanceType)
        # get standby instance number
        standbyDbNode = clusterInfo.getDbNodeByName(standbyInst.hostname)
        if standbyDbNode is None:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                            ("database node configuration on host [%s]"
                             % standbyInst.hostname))
        standbyDataNum = standbyDbNode.getDnNum(standbyInst.instanceType)
        # get dummy instance number
        if dummyStandbyInst is not None:
            dummyDbNode = clusterInfo.getDbNodeByName(
                dummyStandbyInst.hostname)
            if dummyDbNode is None:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                                ("database node configuration on host [%s]"
                                 % dummyStandbyInst.hostname))
            dummyDataNum = dummyDbNode.getDnNum(dummyStandbyInst.instanceType)
        for i in range(channelCount):
            if (dbInst.instanceType == DefaultValue.MASTER_INSTANCE):
                if (i > 0):
                    connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d localservice=%s " \
                             "remotehost=%s remoteport=%d remoteservice=%s" % \
                             (dbInst.haIps[i], dbInst.haPort,
                              (dbInst.port + masterDataNum * 4),
                              standbyInst.haIps[i],
                              standbyInst.haPort, (standbyInst.port +
                                                   standbyDataNum * 4))
                if dummyStandbyInst is not None:
                    if (i > 0):
                        connInfo2 += ","
                    connInfo2 += "localhost=%s localport=%d localservice=%s " \
                                 "remotehost=%s remoteport=%d " \
                                 "remoteservice=%s" % \
                                 (dbInst.haIps[i], dbInst.haPort,
                                  (dbInst.port + masterDataNum * 4),
                                  dummyStandbyInst.haIps[i],
                                  dummyStandbyInst.haPort,
                                  (dummyStandbyInst.port + dummyDataNum * 4))
            elif dbInst.instanceType == DefaultValue.STANDBY_INSTANCE:
                if i > 0:
                    connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d " \
                             "localservice=%s remotehost=%s remoteport=%d " \
                             "remoteservice=%s" % \
                             (dbInst.haIps[i], dbInst.haPort,
                              (dbInst.port + standbyDataNum * 4),
                              masterInst.haIps[i], masterInst.haPort,
                              (masterInst.port + masterDataNum * 4))
                if (dummyStandbyInst is not None):
                    if i > 0:
                        connInfo2 += ","
                    connInfo2 += "localhost=%s localport=%d localservice=%s " \
                                 "remotehost=%s remoteport=%d " \
                                 "remoteservice=%s" % \
                                 (dbInst.haIps[i], dbInst.haPort,
                                  (dbInst.port + standbyDataNum * 4),
                                  dummyStandbyInst.haIps[i],
                                  dummyStandbyInst.haPort,
                                  (dummyStandbyInst.port + dummyDataNum * 4))
            elif (dbInst.instanceType == DefaultValue.DUMMY_STANDBY_INSTANCE):
                if i > 0:
                    connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d localservice=%s " \
                             "remotehost=%s remoteport=%d remoteservice=%s" % \
                             (dbInst.haIps[i], dbInst.haPort,
                              (dbInst.port + dummyDataNum * 4),
                              masterInst.haIps[i],
                              masterInst.haPort,
                              (masterInst.port + masterDataNum * 4))
                if i > 0:
                    connInfo2 += ","
                connInfo2 += "localhost=%s localport=%d " \
                             "localservice=%s remotehost=%s remoteport=%d " \
                             "remoteservice=%s" % \
                             (dbInst.haIps[i], dbInst.haPort,
                              (dbInst.port + dummyDataNum * 4),
                              standbyInst.haIps[i], standbyInst.haPort,
                              (standbyInst.port + standbyDataNum * 4))

        return connInfo1, connInfo2, dummyStandbyInst, nodename

    @staticmethod
    def getInstanceInfoForSinglePrimaryMultiStandbyCluster(dbInst, peerInsts):
        """
        function: get the instance name, master instance and standby
                  instance list
        input : dbInst
        output: NA
        """
        masterInst = None
        standbyInstIdLst = []
        instancename = ""
        # init masterInst, standbyInst
        for pi in iter(peerInsts):
            if pi.instanceType == DefaultValue.MASTER_INSTANCE:
                masterInst = pi
            elif pi.instanceType == DefaultValue.STANDBY_INSTANCE or \
                    dbInst.instanceType == DefaultValue.CASCADE_STANDBY:
                standbyInstIdLst.append(pi.instanceId)

        if dbInst.instanceType == DefaultValue.MASTER_INSTANCE:
            masterInst = dbInst
            instancename = "dn_%d" % masterInst.instanceId
            standbyInstIdLst.sort()
            for si in iter(standbyInstIdLst):
                instancename += "_%d" % si
        elif dbInst.instanceType == DefaultValue.STANDBY_INSTANCE or \
              dbInst.instanceType == DefaultValue.CASCADE_STANDBY:
            instancename = "dn_%d" % masterInst.instanceId
            standbyInstIdLst.append(dbInst.instanceId)
            standbyInstIdLst.sort()
            for si in iter(standbyInstIdLst):
                instancename += "_%d" % si
        return (instancename, masterInst, standbyInstIdLst)

    @staticmethod
    def setReplConninfoForSinglePrimaryMultiStandbyCluster(dbInst,
                                                           peerInsts,
                                                           clusterInfo):
        """
        function: Modify replconninfo for datanode
        input : dbInst
        output: NA
        """
        masterInst = None
        standbyInstIdLst = []
        nodename = ""
        connInfo1 = []
        (nodename, masterInst, standbyInstIdLst) = ClusterInstanceConfig. \
            getInstanceInfoForSinglePrimaryMultiStandbyCluster(dbInst,
                                                               peerInsts)
        if len(masterInst.haIps) == 0:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51621"] +
                            " Data directory: %s." % dbInst.datadir)
        if len(standbyInstIdLst) == 0:
            return connInfo1, nodename

        dbNode = clusterInfo.getDbNodeByName(dbInst.hostname)
        if dbNode is None:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                            ("database node configuration on host [%s]"
                             % dbInst.hostname))

        channelCount = len(masterInst.haIps)
        if dbInst.instanceType == DefaultValue.MASTER_INSTANCE:
            for pj in iter(peerInsts):
                peerDbNode = clusterInfo.getDbNodeByName(pj.hostname)
                if peerDbNode is None:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                                    ("database node configuration on host [%s]"
                                     % pj.hostname))
                chanalInfo = ""
                for i in range(channelCount):
                    if i > 0:
                        chanalInfo += ","
                    chanalInfo += "localhost=%s localport=%d " \
                                  "localheartbeatport=%d localservice=%s " \
                                  "remotehost=%s remoteport=%d " \
                                  "remoteheartbeatport=%d remoteservice=%s" % \
                                  (dbInst.haIps[i], dbInst.haPort,
                                   dbInst.port + 5,
                                   (dbInst.port + 4), pj.haIps[i],
                                   pj.haPort, pj.port + 5,
                                   pj.port + 4)
                    
                connInfo1.append(chanalInfo)
        else:
            for pj in iter(peerInsts):
                peerDbNode = clusterInfo.getDbNodeByName(pj.hostname)
                if peerDbNode is None:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                                    ("database node configuration on host [%s]"
                                     % pj.hostname))
                chanalInfo = ""
                for i in range(channelCount):
                    if i > 0:
                        chanalInfo += ","
                    chanalInfo += "localhost=%s localport=%d " \
                                  "localheartbeatport=%d localservice=%s " \
                                  "remotehost=%s remoteport=%d " \
                                  "remoteheartbeatport=%d remoteservice=%s" % \
                                  (dbInst.haIps[i], dbInst.haPort,
                                   dbInst.port + 5,
                                   (dbInst.port + 4), pj.haIps[i],
                                   pj.haPort, pj.port + 5,
                                   (pj.port + 4))

                connInfo1.append(chanalInfo)

        return connInfo1, nodename


class TempfileManagement():
    """
    create and remove temp file or directory
    """

    def __init__(self):
        """
        function: init function
        input: NA
        output: NA
        """
        pass

    @staticmethod
    def getTempDir(dirName):
        """
        function: create temp directory in PGHOST
        input: dirName
        output:
              pathName
        """
        tmpPath = DefaultValue.getTmpDirFromEnv()
        pathName = os.path.join(tmpPath, dirName)
        return pathName

    @staticmethod
    def removeTempFile(filename, Fuzzy=False):
        """
        function: remove temp files in PGHOST
        input:
              fileName string  Specified file name or keywords
              Fuzzy    bool    Whether to remove files with the same prefix,
              default is False
        output: NA
        """

        if Fuzzy:
            keywords = filename + "*"
            g_file.removeFile(keywords, "shell")
        else:
            g_file.removeFile(filename)
