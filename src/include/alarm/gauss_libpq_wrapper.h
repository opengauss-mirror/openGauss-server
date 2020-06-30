/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * gauss_libpq_wrapper.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gauss_libpq_wrapper.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GS_GAUSS_WRAPPER_H__
#define __GS_GAUSS_WRAPPER_H__

#include "alarm/gs_config.h"
#include "alarm/gs_server.h"
#include "alarm/gt_threads.h"

/* Table name to create and use.  If you are changing,
    also check for create table function */
#define WARN_TABLENAME "gswarnings"
#define WARN_CREATE_TABLE_QRY                                          \
    "CREATE TABLE IF NOT EXISTS " WARN_TABLENAME "(moduleid integer, " \
    "submoduleid integer, "                                            \
    "alarmid bigint, "                                                 \
    "params varchar(512), "                                            \
    "isresolved boolean, "                                             \
    "warning_time timestamp, "                                         \
    "resolve_time timestamp); "

/* Have to bind module,submodule,alarm,param, ==> yet to add "warningtime" */
#define WARN_INSERT_TABLE_QRY \
    "INSERT INTO " WARN_TABLENAME " (moduleid, submoduleid, alarmid, params) VALUES ($1, $2, $3, $4);"

#define WARN_UPDATE_QRY                                                                   \
    "UPDATE " WARN_TABLENAME " SET isresolved=true where moduleid=$1 and submoduleid=$2 " \
    "and alarmid=$3;"

WARNERRCODE gaussConnection(LP_GS_CONFIG config, char* password, LP_GAUSS_CONN* conn);

void gaussDisconnect(LP_GAUSS_CONN* conn);

WARNERRCODE createWarningTableIfNotExits(LP_GAUSS_CONN conn);

WARNERRCODE insertWarning(LP_GAUSS_CONN conn, LP_GS_ALARM alarm);

WARNERRCODE isOpenWarningExists(LP_GAUSS_CONN conn, LP_GS_ALARM alarm, bool* isExists);

WARNERRCODE isOpenWarningExists(LP_GAUSS_CONN conn, LP_GS_ALARM alarm, bool* isExists);

WARNERRCODE isOpenWarningExistInLast3Mins(LP_GAUSS_CONN conn, LP_GS_ALARM alarm, bool* isExists);

WARNERRCODE updateWarningStatusInDB(LP_GAUSS_CONN conn, LP_GS_ALARM alarm);

#endif  //__GS_GAUSS_WRAPPER_H__
