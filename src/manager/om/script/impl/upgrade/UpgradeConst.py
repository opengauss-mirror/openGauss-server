#-*- coding:utf-8 -*-

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
# Const values
#############################################################################

UPGRADE_TIMEOUT_CLUSTER_START = 600
UPGRADE_TIMEOUT_CLUSTER_STOP = 1800

#because the number is float, so notice the recision
DELTA_NUM = 0.000001
#external action
ACTION_CHOSE_STRATEGY = "chose-strategy"
ACTION_INPLACE_UPGRADE = "inplace-binary-upgrade"
#grey upgrade
ACTION_SMALL_UPGRADE = "small-binary-upgrade"
ACTION_LARGE_UPGRADE = "large-binary-upgrade"
# ACTION_ONLINE_UPGRADE is used for record online upgrade step,
# not really provide this action outside to user,
# if use ACTION_BINARY_UPGRADE, it will confuse with off-line binary upgrade
ACTION_AUTO_UPGRADE = "auto-upgrade"
ACTION_AUTO_ROLLBACK = "auto-rollback"
ACTION_COMMIT_UPGRADE = "commit-upgrade"

ACTION_SYNC_CONFIG = "sync_config"
ACTION_SWITCH_PROCESS = "switch_little_effect_process"
ACTION_SWITCH_BIN = "switch_bin"
ACTION_COPY_CERTS = "copy_certs"
ACTION_CLEAN_INSTALL_PATH = "clean_install_path"

ACTION_TOUCH_INIT_FILE = "touch_init_file"
ACTION_CHECK_VERSION = "check_version"

ACTION_BACKUP_CONFIG = "backup_config"
ACTION_RESTORE_CONFIG = "restore_config"
ACTION_INPLACE_BACKUP = "inplace_backup"
ACTION_INPLACE_RESTORE = "inplace_restore"
ACTION_CHECK_GUC = "check_guc"
ACTION_BACKUP_HOTPATCH = "backup_hotpatch"
ACTION_ROLLBACK_HOTPATCH = "rollback_hotpatch"

OPTION_PRECHECK = "before"
OPTION_POSTCHECK = "after"
INPLACE_UPGRADE_STEP_FILE = "upgrade_step.dat"
GREY_UPGRADE_STEP_FILE = "upgrade_step.csv"
CLUSTER_CMSCONF_FILE = "cluster_cmsconf.json"
CLUSTER_CNSCONF_FILE = "cluster_cnconf.json"
READONLY_MODE = "read_only_mode"

#step flag
BINARY_UPGRADE_NO_NEED_ROLLBACK = -2
INVALID_UPRADE_STEP = -1
#binary upgrade step
BINARY_UPGRADE_STEP_INIT_STATUS = 0
BINARY_UPGRADE_STEP_BACKUP_STATUS = 1
BINARY_UPGRADE_STEP_STOP_NODE = 2
BINARY_UPGRADE_STEP_BACKUP_VERSION = 3
BINARY_UPGRADE_STEP_UPGRADE_APP = 4
BINARY_UPGRADE_STEP_START_NODE = 5
BINARY_UPGRADE_STEP_PRE_COMMIT = 6

ERR_GREP_NO_RESULT = 256


#grey upgrade
class GreyUpgradeStep:
    def __init__(self):
        pass

    (STEP_INIT_STATUS,
     STEP_UPDATE_CATALOG,
     STEP_SWITCH_NEW_BIN,
     STEP_UPGRADE_PROCESS,
     STEP_PRE_COMMIT,
     STEP_BEGIN_COMMIT
     ) = range(0, 6)


BACKUP_DIR_LIST = ['global', 'pg_clog', 'pg_xlog', 'pg_multixact',
                   'pg_replslot', 'pg_notify', 'pg_subtrans', 'pg_cbm',
                   'pg_twophase']

FIRST_GREY_UPGRADE_NUM = 92

UPGRADE_PRECOMMIT_NUM = 0.001
UPGRADE_UNSET_NUM = 0

CMSERVER_GUC_DEFAULT = {"enable_transaction_read_only": "on",
                        "coordinator_heartbeat_timeout": "1800",
                        "instance_failover_delay_timeout": 0,
                        "cmserver_ha_heartbeat_timeout": 8}
CMSERVER_GUC_CLOSE = {"enable_transaction_read_only": "off",
                      "coordinator_heartbeat_timeout": "0",
                      "instance_failover_delay_timeout": 40,
                      "cmserver_ha_heartbeat_timeout": 20}
# Script name
GS_UPGRADECTL = "gs_upgradectl"
# table schema and table name
UPGRADE_SCHEMA = "on_upgrade_69954349032535120"
RECORD_NODE_STEP = "record_node_step"
READ_STEP_FROM_FILE_FLAG = "read_step_from_file_flag"
RECORD_UPGRADE_DIR = "record_app_directory"
OLD = "old"
NEW = "new"
# upgrade sql sha file and sql file
UPGRADE_SQL_SHA = "upgrade_sql.sha256"
UPGRADE_SQL_FILE = "upgrade_sql.tar.gz"

COMBIN_NUM = 30
ON_INPLACE_UPGRADE = "IsInplaceUpgrade"
MAX_APP_SIZE = 2000
