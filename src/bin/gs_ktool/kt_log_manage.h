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
 * -------------------------------------------------------------------------
 *
 * kt_log_manage.h
 *     Log manage module : 
 *     1. reading and writing log file;
 *     2. providing log management interface. 
 *
 * IDENTIFICATION
 *      src/bin/gs_ktool/kt_log_manage.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_KT_LOG_MANAGE_H
#define GS_KT_LOG_MANAGE_H

#include "kt_common.h"

const int MAX_OP_LEN = 256;
const int MAX_OP_RESULT_LEN = 4096;
const int MAX_LOG_TYPE_STR_LEN = 15;
const int MAX_OBJECT_LEN = 15;
const int MAX_LOG_CLASS_STR_LEN = 15;

enum LogClass {
    KT_ERROR = 0,
    KT_SUCCEED,
    KT_ALARM,
    KT_WARNING,
    KT_NOTICE,
    KT_HINT,
};

enum KtLogType {
    L_CREATE_FILE = 0,
    L_INIT_KMC,

    /* cmk manage */
    L_GEN_CMK,
    L_DEL_CMK,
    L_SELECT_CMK,
    L_EXPORT_CMK,
    L_IMPORT_CMK,
    L_SELECT_CMK_LEN,
    L_SELECT_CMK_PLAIN,

    /* rk manage */
    L_SELECT_RK,
    L_UPDATE_RK,
    L_SET_RK_VALIDITY,
    L_CHECK_RK_VALIDITY,

    /* invalid cmd input */
    L_PARSE_USER_INPUT,
    L_CHECK_USER_INPUT,

    /* invalid conf file input */
    L_READ_CONF,
    L_SET_CONF,

    /* sys call */
    L_REG_CALLBACK_FUNC,
    L_MALLOC_MEMORY,
    L_GET_TIME,
    L_OPEN_FILE,
    L_GET_ENV_VALUE,
    L_CHECK_ENV_VALUE,
    L_OPEN_SEM,
    L_INIT_SEM,
    L_POST_SEM,
    L_WAIT_SEM,
};

typedef struct KtLog {
    int log_type;
    char operation[MAX_OP_LEN];
    char object[MAX_OBJECT_LEN];
    char level[MAX_LOG_CLASS_STR_LEN];
} KtLog;

extern bool g_has_init_log_module;

#define insert_format_log(log_type, log_class, format_info, ...)                 \
    {                                                                            \
        errno_t def_rc = 0;                                                      \
        char res[MAX_LOG_RESULT_LEN] = {0};                                      \
        def_rc = sprintf_s(res, MAX_LOG_RESULT_LEN, format_info, ##__VA_ARGS__); \
        securec_check_ss_c(def_rc, "", "");                                      \
        insert_raw_log(log_type, log_class, res);                                \
    }

extern bool initialize_logmodule();
extern void insert_raw_log(const KtLogType log_type, const LogClass log_class, const char op_info[MAX_OP_RESULT_LEN]);

#endif