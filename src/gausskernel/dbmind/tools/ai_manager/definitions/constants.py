#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : constants.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : constants.py
#############################################################################
import os
import sys

sys.path.append(sys.path[0] + "/../")
from config_cabin.config import ENV_FILE


class Constant(object):
    # path
    PACK_PATH_PREFIX = 'AI_'
    VERSION_FILE = 'version.cfg'
    CRON_PATH = 'ai_manager/tools/set_cron.py'
    TASK_NAME_LIST = ['agent', 'server', 'monitor']
    REMOTE_DEPLOY_SCRIPT = 'bin/deploy.sh'
    REMOTE_EXECUTE_SCRIPT = 'bin/execute.sh'
    REMOTE_COMMANDER = 'bin/remote_commander'
    AI_MANAGER_PATH = 'ai_manager'
    AI_LIB_PATH = 'envs/ai_lib'
    # encrypt tool path
    ENCRYPT_TOOL = 'encrypt'
    ANOMALY_DETECTION_INSTALL_SCRIPT_PATH = 'ai_manager/module/anomaly_detection/install.py'
    ANOMALY_DETECTION_UNINSTALL_SCRIPT_PATH = 'ai_manager/module/anomaly_detection/uninstall.py'
    # anomaly_detection config path
    ANOMALY_DETECTION_CONFIG_PATH = 'ai_server/dbmind.conf'
    CA_CONFIG = 'config/ca.conf'
    PWF_PATH = 'certificate/pwf'
    TEMP_ANOMALY_PARAM_FILE = 'param_file.json'
    # CERTIFICATE
    CA_ROOT_REQ = 'ca_root.req'
    CA_REQ = 'ca.req'
    CA_ROOT_VALID_DATE = '7300'
    CA_VALID_DATE = '7000'

    # anomaly_detection config item
    AD_CONF_SECTION_SECURITY = 'security'
    AD_CONF_TLS_FLAG = 'tls'
    AD_CONF_CA_PATH = 'ca'
    AD_CONF_SERVER_CERT = 'server_cert'
    AD_CONF_SERVER_KEY = 'server_key'
    AD_CONF_AGENT_CERT = 'agent_cert'
    AD_CONF_AGENT_KEY = 'agent_key'
    AD_CONF_SECTION_DATABASE = 'database'
    AD_CONF_DATABASE_PATH = 'database_path'

    # default values
    VERSION_FILE_MAX_LINES = 100

    # anomaly detection param name
    MODULE = 'module'
    AI_SERVER = 'ai_server'
    MODULE_ANOMALY_DETECTION = 'anomaly_detection'
    AGENT_NODES = 'agent_nodes'
    NODE_IP = 'node_ip'
    NODE_USER = 'username'
    NODE_PWD = 'password'
    PACKAGE_PATH = 'package_path'
    # ca information in param file
    CA_INFO = 'ca_info'
    CA_CERT_PATH = 'ca_cert_path'
    CA_KEY_PATH = 'ca_key_path'
    CA_PASSWORD = 'ca_password'

    # index advisor param name
    INSTALL_NODES = 'install_nodes'

    # cmd prefix
    CMD_PREFIX = 'source ~/.bashrc; source %s &&' % ENV_FILE
    EXECUTE_REMOTE_SCRIPT = '\'%spython3 %s --param_file %s\''

    # anormaly_detection_install_cron
    ANORMALY_MAIN_SCRIPT = 'ai_server/main.py'
    ANOMALY_INSTALL_CRON_LOCALLY = [
        "%s cd %s && nohup python3 %s start --role server"
    ]
    ANOMALY_STOP_CMD_LOCALLY = [
        "%s cd %s && nohup python3 %s stop --role server"
    ]
    OPENGAUSS_ANOMALY_INSTALL_CRON_LOCALLY = [
        "%s cd %s && nohup python3 %s start --role server"
    ]
    OPENGAUSS_ANOMALY_STOP_CMD_LOCALLY = [
        "%s cd %s && nohup python3 %s stop --role server"
    ]
    OPENGAUSS_ANOMALY_INSTALL_CRON_REMOTE = [
        "%s cd %s && nohup python3 %s start --role agent"
    ]
    OPENGAUSS_ANOMALY_STOP_CMD_REMOTE = [
        "%s cd %s && nohup python3 %s stop --role agent"
    ]

    # expected content
    CRON_INFO_EXPECTED = "no crontab for"
    SCENE_HUAWEIYUN = "huaweiyun"
    SCENE_OPENGAUSS = "opengauss"

    # execute result
    SUCCESS = 'Successfully'
    FAILED = 'Failed'

    # wait time
    DEFAULT_WAIT_SECONDS = 70

    # random passwd len
    RANDOM_PASSWD_LEN = 12

    # valid check
    VALID_MODULE_NAME = ['anomaly_detection']
    VALID_ACTION_NAME = ['install', 'uninstall']
    VALID_BOOL_TYPE = ['True', 'False']
    PATH_CHECK_LIST = ["|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                       "{", "}", "(", ")", "[", "]", "~", "*", "?", " ", "!", "\n"]
    CMD_CHECK_LIST = ["|", ";", "&", "<", ">", "`", "\\", "!", "\n"]
    VALID_LOG_LEVEL = [1, 2, 3, 4, 5]
    VALID_CONFIG_SECTION_OPT_MAPPING = {
        'server': ['host', 'listen_host', 'listen_port', 'pull_kafka'],
        'database': ['name', 'host', 'port', 'user', 'size', 'max_rows'],
        'agent': ['cluster_name', 'collection_item', 'source_timer_interval',
                  'sink_timer_interval', 'channel_capacity', 'collection_type'],
        'security': ['tls'],
        'log': ['log_path']
    }
    VALID_SCENE = ['huaweiyun', 'opengauss']
    VALID_CA_INFO = ['ca_cert_path', 'ca_key_path', 'ca_password']
    VALID_COLLECTION_TYPE = ['os', 'database', 'all']
    VALID_COLLECT_DATA_TYPE = ['dn', 'cn']
    # check items
    CHECK_PORT = ['listen_port', 'port']
    CHECK_IP = ['host', 'listen_host', 'host']
    # valid database name
    VALID_DATABASE_NAME = ['sqlite', 'influxdb', 'mongodb']
    # essential params check
    CONFIG_INFO = 'config_info'
    SERVER = 'server'
    SERVER_HOST = 'host'
    SERVER_LISTEN_HOST = 'listen_host'
    SERVER_LISTEN_PORT = 'listen_port'
    SERVER_PULL_KAFKA = 'pull_kafka'
    DATABASE = 'database'
    DATABASE_NAME = 'name'
    DATABASE_HOST = 'host'
    DATABASE_PORT = 'port'
    DATABASE_USER = 'user'
    DATABASE_SIZE = 'size'
    DATABASE_MAX_ROWS = 'max_rows'
    AGENT = 'agent'
    AGENT_CLUSTER_NAME = 'cluster_name'
    AGENT_COLLECTION_TYPE = 'collection_type'
    AGENT_COLLECTION_ITEM = 'collection_item'
    AGENT_CHANNEL_CAPACITY = 'channel_capacity'
    AGENT_SOURCE_TIMER_INTERVAL = 'source_timer_interval'
    AGENT_SINK_TIMER_INTERVAL = 'sink_timer_interval'

    # log
    LOG_FORMATTER = '[%(asctime)s][%(levelname)s][%(pathname)s][%(funcName)s][%(lineno)d][%(message)s]'
    DEFAULT_LOG_NAME = 'ai_manager_log'
    CRON_LOG_NAME = 'cron_log'
    LOG_SEP_LINE = '=' * 50

    # permission code
    AUTH_COMMON_DIR_STR = '750'
    AUTH_COMMON_FILE_STR = '600'
    AUTH_COMMON_ENCRYPT_FILES = '700'

    AUTH_COMMON_FILE = 0o600

    # permission mapping
    AUTHORITY_FULL = {
        'read': os.R_OK,
        'write': os.W_OK,
        'execute': os.X_OK
    }
    AUTHORITY_EXIST = {
        'exist': os.F_OK
    }
    AUTHORITY_RW = {
        'read': os.R_OK,
        'write': os.W_OK
    }
    AUTHORITY_R = {
        'read': os.R_OK
    }
    # cmd lib
    SHELL_CMD_DICT = {
        "deleteFile": "(if [ -f '%s' ];then rm -f '%s';fi)",
        "deleteLibFile": "cd %s && ls | grep -E '%s'|xargs rm -f",
        "cleanDir": "(if [ -d '%s' ];then rm -rf '%s'/* && cd '%s' && ls -A | xargs rm -rf ; fi)",
        "simpleCleanDir": "rm -rf %s/*",
        "execShellFile": "sh %s",
        "getFullPathForShellCmd": "which %s",
        "deleteDir": "(if [ -d '%s' ];then rm -rf '%s';fi)",
        "deleteLib": "(if [ -e '%s' ];then rm -rf '%s';fi)",
        "createDir": "(if [ ! -d '%s' ]; then mkdir -p '%s' -m %s;fi)",
        "createDirSimple": "mkdir -p '%s' -m %s",
        "createFile": "touch '%s' && chmod %s '%s'",
        "deleteBatchFiles": "rm -f %s*",
        "compressTarFile": "cd '%s' && tar -cf '%s' %s && chmod %s '%s' ",
        "decompressTarFile": "cd '%s' && tar -xf '%s' ",
        "decompressFileToDir": "tar -xf '%s' -C '%s'",
        "copyFile": " cp -r %s %s ",
        "renameFile": "(if [ -f '%s' ];then mv '%s' '%s';fi)",
        "cleanFile": "if [ -f %s ]; then echo '' > %s; fi",
        "checkUserPermission": "su - %s -c \"cd '%s'\"",
        "getFileTime": "echo $[`date +%%s`-`stat -c %%Y %s`]",
        "findfiles": "cd %s && find . -type l -print",
        "copyFile1": "(if [ -f '%s' ];then cp '%s' '%s';fi)",
        "copyFile2": "(if [ -f '%s' ] && [ ! -f '%s' ];then cp '%s' '%s';fi)",
        "cleanDir1": "(if [ -d '%s' ]; then cd '%s' && rm -rf '%s' && rm -rf '%s' && cd -; fi)",
        "cleanDir2": "(if [ -d '%s' ]; then rm -rf '%s'/* && cd '%s' && ls -A | xargs rm -rf && cd -; fi)",
        "cleanDir3": "rm -rf '%s'/* && cd '%s' && ls -A | xargs rm -rf && cd - ",
        "cleanDir4": "rm -rf %s/*",
        "checkNodeConnection": "ping %s -i 1 -c 3 |grep ttl |wc -l",
        "overWriteFile": "echo '%s' > '%s'",
        "physicMemory": "cat /proc/meminfo | grep MemTotal",
        "findFile": "(if [ -d '%s' ]; then find '%s' -type f;fi)",
        "unzipForce": "unzip -o '%s' -d '%s'",
        "sleep": "sleep %s",
        "softLink": "ln -s '%s' '%s'",
        "findwithcd": "cd %s && find ./ -name %s",
        "changeMode": "chmod %s %s",
        "checkPassword": "export LC_ALL=C; chage -l %s | grep -i %s",
        "changeModeForFiles": "chmod -R %s %s/*",
        "addCronCMD": "%spython3 %s -t add -c %s \"%s\"",
        "delCronCMD": "%spython3 %s -t del -c %s \"%s\"",
        "killProcess": "ps ux | grep '%s' | grep -v grep | awk '{print $2}' | xargs kill -9",
        "checkProcess": "unset LD_LIBRARY_PATH && ps ux | grep '%s' | grep -v grep | wc -l",
        "executeShellScripCmd": "echo %s | sh %s %s %s %s %s",
        "executeShellScripCmd1": "echo %s | sh %s %s %s %s",
        "getUser": "echo $USER",
        "ifconfig": "ifconfig",
        "chmodWithExecute": "chmod +x %s",
        "remoteDeploy": "echo %s | sh %s \"scp -r %s %s@%s:%s\"",
        "remoteExecute": "echo %s | sh %s \"ssh %s@%s %s\"",
        "showDirDocs": "ls %s | wc -l"
    }


