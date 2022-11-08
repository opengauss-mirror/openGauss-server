/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_dms_log_output.cpp
 *  write log for dms
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_log_output.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "utils/elog.h"
#include "knl/knl_thread.h"
#include "ddes/dms/ss_dms_log_output.h"

void DMSLogOutput(uint32 ss_log_level, const char *code_file_name, uint32 code_line_num, char buf[])
{
    int saved_log_output = t_thrd.postgres_cxt.whereToSendOutput;
    if (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER) {
        t_thrd.postgres_cxt.whereToSendOutput = (int)DestNone;
    }
    int32 log_level;
    switch (ss_log_level) {
        case LOG_RUN_ERR_LEVEL:
        case LOG_DEBUG_ERR_LEVEL:
            /* avoid error->fatal,  proc_exit() infinite loop and deadlocks */
            log_level = WARNING;
            break;
        case LOG_RUN_WAR_LEVEL:
        case LOG_DEBUG_WAR_LEVEL:
            log_level = WARNING;
            break;
        case LOG_RUN_INF_LEVEL:
        case LOG_DEBUG_INF_LEVEL:
            log_level = ENABLE_SS_LOG ? LOG : DEBUG1;
            break;
        default:
            log_level = DEBUG1;  // it will be DEBUG level later
            break;
    }
    ereport(log_level, (errmodule(MOD_DMS), errmsg("%s:%u  %s", code_file_name, code_line_num, buf)));
    if (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER) {
        t_thrd.postgres_cxt.whereToSendOutput = saved_log_output;
    }
}

int32 DMSLogLevelCheck(dms_log_id_t dms_log_id, dms_log_level_t dms_log_level, uint32 *log_level)
{
    static uint32 db_log_map[DMS_LOG_ID_COUNT][DMS_LOG_LEVEL_COUNT] = {
        {LOG_RUN_ERR_LEVEL, LOG_RUN_WAR_LEVEL, LOG_RUN_INF_LEVEL},
        {LOG_DEBUG_ERR_LEVEL, LOG_DEBUG_WAR_LEVEL, LOG_DEBUG_INF_LEVEL}
    };

    if (dms_log_id >= DMS_LOG_ID_COUNT || dms_log_level >= DMS_LOG_LEVEL_COUNT) {
        return -1;
    }
    *log_level = db_log_map[dms_log_id][dms_log_level];
    return 0;
}