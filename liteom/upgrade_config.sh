#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# date: 2021-12-22
# version: 1.0

# 数据库监听端口
GAUSS_LISTEN_PORT=""

# 数据库管理员用户名
GAUSS_ADMIN_USER=""

#数据库升级回退日志路径
GAUSS_LOG_PATH=""

#数据库升级根位置
GAUSS_UPGRADE_BASE_PATH=""

#数据库SQL包位置
GAUSS_SQL_TAR_PATH=""

#数据库低版本备份位置
GAUSS_BACKUP_BASE_PATH=""

#数据库临时目录
GAUSS_TMP_PATH=""

#是否使用存在的bin解压包
GAUSS_UPGRADE_BIN_PATH=""

#需要同步的cluster config 列表
GAUSS_UPGRADE_SYNC_CONFIG_LIST=""