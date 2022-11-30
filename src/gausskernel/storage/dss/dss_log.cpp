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
 * dss_log.cpp
 *  write log for dss
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dss/dss_log.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/palloc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "knl/knl_thread.h"
#include "storage/dss/dss_api_def.h"
#include "storage/dss/dss_log.h"
#include "storage/dss/dss_adaptor.h"

static void dss_log_report(uint32 dss_log_level, const char *code_file_name, uint32 code_line_num, const char buf[])
{
    int saved_log_output = t_thrd.postgres_cxt.whereToSendOutput;
    if (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER) {
        t_thrd.postgres_cxt.whereToSendOutput = (int)DestNone;
    }
    int32 log_level;
    switch (dss_log_level) {
        /* In view of the differences between DSSAPI and glibc, we record the DSS_ERROR as WARNING */
        case LOG_RUN_ERR_LEVEL:
        case LOG_DEBUG_ERR_LEVEL:
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
            log_level = DEBUG1;  // it will be DEBUG level later.
            break;
    }

    ereport(log_level,
        (errmodule(MOD_DSS),
            errmsg("%s:%u  %s", code_file_name, code_line_num, buf)));
    if (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER) {
        t_thrd.postgres_cxt.whereToSendOutput = saved_log_output;
    }
}

static int32 dss_log_check(dss_log_id_t dss_log_id, dss_log_level_t dss_log_level, uint32 *log_level)
{
    static uint32 db_log_map[DSS_LOG_ID_COUNT][DSS_LOG_LEVEL_COUNT] = {
        {LOG_RUN_ERR_LEVEL, LOG_RUN_WAR_LEVEL, LOG_RUN_INF_LEVEL},
        {LOG_DEBUG_ERR_LEVEL, LOG_DEBUG_WAR_LEVEL, LOG_DEBUG_INF_LEVEL}
    };

    if (dss_log_id >= DSS_LOG_ID_COUNT || dss_log_level >= DSS_LOG_LEVEL_COUNT) {
        return -1;
    }

    *log_level = db_log_map[dss_log_id][dss_log_level];

    return 0;
}

static void dss_write_normal_log(dss_log_id_t dss_log_id, dss_log_level_t dss_log_level, const char *code_file_name,
    uint32 code_line_num, const char *module_name, const char *format, ...)
{
    int32 errcode;
    uint32 log_level;
    const char *last_file = NULL;

    int32 ret = dss_log_check(dss_log_id, dss_log_level, &log_level);
    if (ret == -1) {
        return;
    }

#ifdef WIN32
    last_file = strrchr(code_file_name, '\\');
#else
    last_file = strrchr(code_file_name, '/');
#endif
    if (last_file == NULL) {
        last_file = code_file_name;
    } else {
        last_file++;
    }

    va_list args;
    va_start(args, format);
    char buf[DMS_LOGGER_BUFFER_SIZE];
    errcode = vsnprintf_s(buf, DMS_LOGGER_BUFFER_SIZE, DMS_LOGGER_BUFFER_SIZE, format, args);
    if (errcode < 0) {
        va_end(args);
        return;
    }
    va_end(args);

    int saveInterruptHoldoffCount = (int)t_thrd.int_cxt.InterruptHoldoffCount;
    MemoryContext old_context = MemoryContextSwitchTo(ErrorContext);
    PG_TRY();
    {
        dss_log_report(log_level, last_file, code_line_num, buf);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = (uint32)saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();
    (void)MemoryContextSwitchTo(old_context);
}

void dss_log_init(void)
{
    dss_register_log_callback(dss_write_normal_log);
}