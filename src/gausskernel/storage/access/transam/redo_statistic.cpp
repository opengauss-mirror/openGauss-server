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
 * redo_statistic.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/redo_statistic.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <unistd.h>
#include "utils/elog.h"
#include "utils/builtins.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "access/redo_statistic.h"
#include "miscadmin.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/multi_redo_api.h"
#include "instruments/instr_waitevent.h"
#include "access/parallel_recovery/spsc_blocking_queue.h"
#include "storage/copydir.h"

static const uint32 MAX_REALPATH_LEN = 4096;
Datum redo_get_node_name()
{
    return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
}

Datum redo_get_start_ptr()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.redo_start_ptr);
}

Datum redo_get_start_time()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.redo_start_time);
}

Datum redo_get_done_time()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time);
}

Datum redo_get_current_time()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.oldest_segment);
}

Datum redo_get_min_recovery_point()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.min_recovery_point);
}

Datum redo_get_read_ptr()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.read_ptr);
}

Datum redo_get_last_replayed_read_Ptr()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.last_replayed_end_ptr);
}

Datum redo_get_recovery_done()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr);
}

Datum redo_get_read_xlog_io_counter()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_READ_XLOG].counter);
}

Datum redo_get_read_xlog_io_total_dur()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_READ_XLOG].total_duration);
}

Datum redo_get_read_data_io_counter()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_READ_DATA].counter);
}

Datum redo_get_read_data_io_total_dur()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_READ_DATA].total_duration);
}

Datum redo_get_write_data_io_counter()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_WRITE_DATA].counter);
}

Datum redo_get_write_data_io_total_dur()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_WRITE_DATA].total_duration);
}

Datum redo_get_process_pending_counter()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_PROCESS_PENDING].counter);
}

Datum redo_get_process_pending_total_dur()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_PROCESS_PENDING].total_duration);
}

Datum redo_get_apply_counter()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_APPLY].counter);
}

Datum redo_get_apply_total_dur()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.wait_info[WAIT_APPLY].total_duration);
}

Datum redo_get_speed()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.speed_according_seg);
}

Datum redo_get_local_max_lsn()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.local_max_lsn);
}

Datum redo_get_primary_flush_ptr()
{
    return UInt64GetDatum(g_instance.comm_cxt.predo_cxt.redoPf.primary_flush_ptr);
}

WaitEventIO redo_get_event_type_by_wait_type(uint32 type)
{
    switch (type) {
        case WAIT_READ_XLOG:
            return WAIT_EVENT_WAL_READ;
        case WAIT_READ_DATA:
            return WAIT_EVENT_DATA_FILE_READ;
        case WAIT_WRITE_DATA:
            return WAIT_EVENT_DATA_FILE_WRITE;
        case WAIT_PROCESS_PENDING:
            return WAIT_EVENT_PREDO_PROCESS_PENDING;
        case WAIT_APPLY:
            return WAIT_EVENT_PREDO_APPLY;
        default:
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_STATS]redo_get_event_type_by_wait_type: input type:%u is unknown.", type)));
            return WAIT_EVENT_PREDO_APPLY;
    }
}

char *redo_get_name_by_wait_type(uint32 type)
{
    switch (type) {
        case WAIT_READ_XLOG:
            return "WAIT_READ_XLOG";
        case WAIT_READ_DATA:
            return "WAIT_READ_DATA";
        case WAIT_WRITE_DATA:
            return "WAIT_WRITE_DATA";
        case WAIT_PROCESS_PENDING:
            return "WAIT_PROCESS_PENDING";
        case WAIT_APPLY:
            return "WAIT_APPLY";
        default:
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_STATS]redo_get_event_type_by_wait_type: input type:%u is unknown.", type)));
            return "UNKNOWN_TYPE";
    }
}

static inline uint32 MinNumber(uint32 a, uint32 b)
{
    return (b < a ? b : a);
}

void redo_get_worker_info_text(char *info, uint32 max_info_len)
{
    RedoWorkerStatsData worker[MAX_RECOVERY_THREAD_NUM] = {0};
    uint32 worker_num = 0;
    errno_t errorno = EOK;
    GetRedoWrokerStatistic(&worker_num, worker, MinNumber((uint32)MAX_RECOVERY_THREAD_NUM, max_info_len));

    if (worker_num == 0) {
        errorno = snprintf_s(info, max_info_len, max_info_len - 1, "%-16s", "no redo worker");
        securec_check_ss(errorno, "\0", "\0");
        return;
    }
    errorno = snprintf_s(info, max_info_len, max_info_len - 1, "%-4s%-8s%-11s%-21s", "id", "q_use", "q_max_use",
                         "rec_cnt");
    securec_check_ss(errorno, "\0", "\0");
    for (uint32 i = 0; i < worker_num; ++i) {
        errorno = snprintf_s(info + strlen(info), max_info_len - strlen(info), max_info_len - strlen(info) - 1,
                             "\n%-4u%-8u%-11u%-21lu", worker[i].id, worker[i].queue_usage, worker[i].queue_max_usage,
                             worker[i].redo_rec_count);
        securec_check_ss(errorno, "\0", "\0");
    }
}

Datum redo_get_worker_info()
{
    Datum value;
    char *info = (char *)palloc0(sizeof(char) * REDO_WORKER_INFO_BUFFER_SIZE);
    redo_get_worker_info_text(info, REDO_WORKER_INFO_BUFFER_SIZE);
    value = CStringGetTextDatum(info);
    pfree_ext(info);
    return value;
}

/* redo statistic view */
const RedoStatsViewObj g_redoViewArr[REDO_VIEW_COL_SIZE] = {
    { "node_name", TEXTOID, redo_get_node_name },
    { "redo_start_ptr", INT8OID, redo_get_start_ptr },
    { "redo_start_time", INT8OID, redo_get_start_time },
    { "redo_done_time", INT8OID, redo_get_done_time },
    { "curr_time", INT8OID, redo_get_current_time },

    { "min_recovery_point", INT8OID, redo_get_min_recovery_point },
    { "read_ptr", INT8OID, redo_get_read_ptr },
    { "last_replayed_read_ptr", INT8OID, redo_get_last_replayed_read_Ptr },
    { "recovery_done_ptr", INT8OID, redo_get_recovery_done },
    { "read_xlog_io_avg_dur", INT8OID, redo_get_read_xlog_io_counter },

    { "read_xlog_io_total_dur", INT8OID, redo_get_read_xlog_io_total_dur },
    { "read_data_io_avg_dur", INT8OID, redo_get_read_data_io_counter },
    { "read_data_io_total_dur", INT8OID, redo_get_read_data_io_total_dur },
    { "write_data_io_avg_dur", INT8OID, redo_get_write_data_io_counter },
    { "write_data_io_total_dur", INT8OID, redo_get_write_data_io_total_dur },

    { "process_pending_avg_dur", INT8OID, redo_get_process_pending_counter },
    { "process_pending_total_dur", INT8OID, redo_get_process_pending_total_dur },
    { "apply_avg_dur", INT8OID, redo_get_apply_counter },
    { "apply_total_dur", INT8OID, redo_get_apply_total_dur },
    { "speed", INT8OID, redo_get_speed },

    { "local_max_ptr", INT8OID, redo_get_local_max_lsn },
    { "primary_flush_ptr", INT8OID, redo_get_primary_flush_ptr },
    { "worker_info", TEXTOID, redo_get_worker_info }
};

void print_stats_file(RedoStatsData *stats)
{
    uint32 type;
    if (!(module_logging_is_on(MOD_REDO))) {
        return;
    }
    ereport(
        LOG,
        (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
         errmsg("[REDO_STATS]print_stats_file: the basic statistic during redo are as follows : "
                "redo_start_ptr:%lu, redo_start_time:%ld, redo_done_time:%ld, curr_time:%ld, min_recovery_point:%lu, "
                "read_ptr:%lu, last_replayed_read_Ptr:%lu, recovery_done_ptr:%lu, speed:%u KB/s, local_max_lsn:%lu, "
                "worker_info_len:%u",
                stats->redo_start_ptr, stats->redo_start_time, stats->redo_done_time, stats->curr_time,
                stats->min_recovery_point, stats->read_ptr, stats->last_replayed_read_ptr, stats->recovery_done_ptr,
                stats->speed_according_seg, stats->local_max_lsn, stats->worker_info_len)));

    for (type = 0; type < WAIT_REDO_NUM; type++) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("[REDO_STATS]print_stats_file %s: the event io statistic during redo are as follows : "
                             "total_duration:%ld, counter:%ld",
                             redo_get_name_by_wait_type(type), stats->wait_info[type].total_duration,
                             stats->wait_info[type].counter)));
    }
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("[REDO_STATS]print_stats_file: redo worker info are as follows :%s", stats->worker_info)));
}

void redo_update_stats_file(RedoStatsData *stats)
{
    FILE *statef = NULL;

    if (stats == NULL) {
        return;
    }

    statef = fopen(REDO_STATS_FILE_TMP, "w");
    if (statef == NULL) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_update_stats_file: fopen temppath:%s failed!", REDO_STATS_FILE_TMP)));
        return;
    }
    if (fwrite(stats, 1, sizeof(RedoStatsData), statef) == 0) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_update_stats_file: fwrite temppath:%s failed!", REDO_STATS_FILE_TMP)));
        fclose(statef);
        return;
    }
    fclose(statef);
    print_stats_file(stats);
    if (durable_rename(REDO_STATS_FILE_TMP, REDO_STATS_FILE, WARNING) != 0) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_update_stats_file: durable_rename:%s failed!", REDO_STATS_FILE_TMP)));
    }
}

void redo_unlink_stats_file()
{
    int ret = 0;

    ret = unlink(REDO_STATS_FILE);
    if (ret < 0 && errno != ENOENT) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_unlink_stats_file: unlink %s failed!, ret:%d", REDO_STATS_FILE, ret)));
    } else {
        ereport(LOG,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_unlink_stats_file: unlink %s sucessfully!, ret:%d", REDO_STATS_FILE, ret)));
    }
}

void redo_get_statistic(RedoStatsData *stat, uint64 speed)
{
    uint32 type;
    Assert(stat != NULL);
    stat->redo_start_ptr = g_instance.comm_cxt.predo_cxt.redoPf.redo_start_ptr;
    stat->redo_start_time = g_instance.comm_cxt.predo_cxt.redoPf.redo_start_time;
    stat->redo_done_time = g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time;
    stat->curr_time = g_instance.comm_cxt.predo_cxt.redoPf.oldest_segment;
    stat->min_recovery_point = g_instance.comm_cxt.predo_cxt.redoPf.min_recovery_point;
    stat->read_ptr = g_instance.comm_cxt.predo_cxt.redoPf.read_ptr;
    stat->last_replayed_read_ptr = g_instance.comm_cxt.predo_cxt.redoPf.last_replayed_end_ptr;
    stat->recovery_done_ptr = g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr;
    stat->speed_according_seg = speed;
    stat->local_max_lsn = g_instance.comm_cxt.predo_cxt.redoPf.local_max_lsn;

    for (type = 0; type < WAIT_REDO_NUM; type++) {
        stat->wait_info[type] = GetRedoIoEvent(redo_get_event_type_by_wait_type(type));
    }

    redo_get_worker_info_text(stat->worker_info, REDO_WORKER_INFO_BUFFER_SIZE);
    stat->worker_info_len = strlen(stat->worker_info);
}

void redo_refresh_stats(uint64 speed)
{
    RedoStatsData *stat = NULL;
    if (t_thrd.xlog_cxt.reachedConsistency == true) {
        return;
    }
    stat = (RedoStatsData *)palloc0(sizeof(RedoStatsData));
    if (stat == NULL) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[REDO_STATS]redo_refresh_stats: allocated RedoStatsData failed!, "
                                 "speed:%lu B/s, allocated len:%lu",
                                 speed, sizeof(RedoStatsData))));
        return;
    }
    redo_get_statistic(stat, speed);
    redo_update_stats_file(stat);
    pfree_ext(stat);
}

void redo_fill_redo_event()
{
    uint32 type;
    for (type = 0; type < WAIT_REDO_NUM; type++) {
        g_instance.comm_cxt.predo_cxt.redoPf.wait_info[type] = GetRedoIoEvent(redo_get_event_type_by_wait_type(type));
    }
}
