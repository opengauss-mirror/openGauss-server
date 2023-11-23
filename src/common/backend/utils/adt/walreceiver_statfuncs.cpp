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
 * walreceiver_statfuncs.cpp
 *    stream main Interface.
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/walreceiver_statfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "knl/knl_variable.h"
#include "postgres.h"
#include "replication/walreceiver.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "utils/statfuncs.h"
#include "catalog/pg_type.h"


#define WAL_RECEIVER_STATS_COLS 10
#define WAL_RECV_WRITER_STATS_COLS 15

typedef void (*TupleValuesBuilder)(TupleDesc tupleDesc, Tuplestorestate* tupstore, int operation);

static uint64 GetCurrentWalReceiverBufferSize();
static Datum StatRoutine(PG_FUNCTION_ARGS, TupleValuesBuilder buildTupleValues);
static void BuildReceiverStatsTupleValues(TupleDesc tupleDesc, Tuplestorestate* tupstore, int operation);
static void BuildRecvWriterStatsTupleValues(TupleDesc tupleDesc, Tuplestorestate* tupstore, int operation);


/**
 * gs_stat_walreceiver：
 *     obtain the running status of WalReceiver.
 * input args:
 *     operation: int, -1: disable, 0: reset, 1: enable, 2: get
 * output columns:
 *     is_enable_stat boolean
 *     buffer_current_size uint64
 *     buffer_full_times uint64
 *     wake_writer_times uint64
 *     avg_wake_interval uint64
 *     since_last_wake_interval uint64
 *     first_wake_time timestamptz
 *     last_wake_time timestamptz
 *     last_reset_time timestamptz
 *     cur_time timestamptz
 */
Datum gs_stat_walreceiver(PG_FUNCTION_ARGS)
{
    return StatRoutine(fcinfo, BuildReceiverStatsTupleValues);
}

/**
 * gs_stat_walrecvwriter
 *     obtain the running status of WalRecvWriter.
 * input args:
 *     operation: int, -1: disable, 0: reset, 1: enable, 2: get
 * output columns:
*     is_enable_stat boolean
*     total_write_bytes uint64
*     write_times uint64
*     total_write_time uint64
*     avg_write_time uint64
*     avg_write_bytes uint64
*     total_sync_bytes uint64
*     sync_times uint64
*     total_sync_time uint64
*     avg_sync_time uint64
*     avg_sync_bytes uint64
*     current_xlog_segno uint64
*     inited_xlog_segno uint64
*     last_reset_time timestamptz
*     cur_time timestamptz
 */
Datum gs_stat_walrecvwriter(PG_FUNCTION_ARGS)
{
    return StatRoutine(fcinfo, BuildRecvWriterStatsTupleValues);
}

static Datum StatRoutine(PG_FUNCTION_ARGS, TupleValuesBuilder buildTupleValues)
{
    TupleDesc tupDesc = nullptr;
    Tuplestorestate* tupstore = BuildTupleResult(fcinfo, &tupDesc);
    const int operation = PG_GETARG_INT32(0);

    switch (operation) {
        case STAT_OPER_GET:
        case STAT_OPER_DISABLE:
        case STAT_OPER_RESET:
        case STAT_OPER_ENABLE:
            buildTupleValues(tupDesc, tupstore, operation);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("illegal value \"%d\" for parameter \"operation\".", operation),
                errhint("-1: disable, 0: reset, 1: enable, 2: get")));
            break;
    }

    PG_RETURN_VOID();
}

static void BuildReceiverStatsTupleValues(TupleDesc tupleDesc, Tuplestorestate* tupstore, int operation)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    Datum values[WAL_RECEIVER_STATS_COLS];
    bool isnulls[WAL_RECEIVER_STATS_COLS] = {false};
    uint64 curSize = GetCurrentWalReceiverBufferSize();

    volatile WalReceiverStats* stats = g_instance.wal_cxt.walReceiverStats;
    SpinLockAcquire(&stats->mutex);
    TimestampTz curTime = GetCurrentTimestamp();
    if (operation == STAT_OPER_ENABLE) {
        stats->isEnableStat = true;
    } else if (operation == STAT_OPER_DISABLE) {
        stats->isEnableStat = false;
    } else if (operation == STAT_OPER_RESET) {
        stats->bufferFullTimes = 0;
        stats->wakeWriterTimes = 0;
        stats->firstWakeTime = 0;
        stats->lastWakeTime = 0;
        stats->lastResetTime = curTime;
    }

    bool isEnableStat = stats->isEnableStat;
    uint64 bufferFullTimes = stats->bufferFullTimes;
    uint64 wakeWriterTimes = stats->wakeWriterTimes;
    TimestampTz firstWakeTime = stats->firstWakeTime;
    TimestampTz lastWakeTime = stats->lastWakeTime;
    TimestampTz lastResetTime = stats->lastResetTime;
    SpinLockRelease(&stats->mutex);

    int colIdx = 0;
    /* "is_enable_stat" */
    values[colIdx++] = BoolGetDatum(isEnableStat);
    /* "buffer_current_size" */
    values[colIdx++] = UInt64GetDatum(curSize);
    /* "buffer_full_times" */
    values[colIdx++] = UInt64GetDatum(bufferFullTimes);
    /* "wake_writer_times" */
    values[colIdx++] = UInt64GetDatum(wakeWriterTimes);

    /* avg_wake_interval */
    uint64 avgWakeInterval;
    if (wakeWriterTimes > 1) {
        avgWakeInterval = (lastWakeTime - firstWakeTime) / (wakeWriterTimes - 1);
        isnulls[colIdx] = false;
    } else {
        avgWakeInterval = 0;
        isnulls[colIdx] = true;
    }
    values[colIdx++] = UInt64GetDatum(avgWakeInterval);

    /* since_last_wake_interval */
    uint64 sinceLastWakeInterval;
    if (wakeWriterTimes > 0) {
        sinceLastWakeInterval = curTime - lastWakeTime;
        isnulls[colIdx] = false;
    } else {
        sinceLastWakeInterval = 0;
        isnulls[colIdx] = true;
    }
    values[colIdx++] = UInt64GetDatum(sinceLastWakeInterval);

    /* first_wake_time */
    isnulls[colIdx] = (firstWakeTime == 0);
    values[colIdx++] = TimestampTzGetDatum(firstWakeTime);

    /* last_wake_time */
    isnulls[colIdx] = (lastWakeTime == 0);
    values[colIdx++] = TimestampTzGetDatum(lastWakeTime);

    /* last_reset_time */
    isnulls[colIdx] = (lastResetTime == 0);
    values[colIdx++] = TimestampTzGetDatum(lastResetTime);

    /* cur_time */
    values[colIdx++] = TimestampTzGetDatum(curTime);

    Assert(WAL_RECEIVER_STATS_COLS == colIdx);

    tuplestore_putvalues(tupstore, tupleDesc, values, isnulls);
    tuplestore_donestoring(tupstore);
}

static uint64 GetCurrentWalReceiverBufferSize()
{
    uint64 curSize;
    WalReceiverStats* stats = g_instance.wal_cxt.walReceiverStats;
    WalRcvCtlBlock* block = (WalRcvCtlBlock*)stats->walRcvCtlBlock;
    if (!block) {
        return 0;
    }

    SpinLockAcquire(&block->mutex);
    int64 walFreeOffset = block->walFreeOffset;
    int64 walWriteOffset = block->walWriteOffset;
    SpinLockRelease(&block->mutex);

    if (walFreeOffset < walWriteOffset) {
        int recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
        curSize = (uint64)(recBufferSize - walWriteOffset + walFreeOffset);
    } else {
        curSize = (uint64)(walFreeOffset - walWriteOffset);
    }
    return curSize;
}

static void BuildRecvWriterStatsTupleValues(TupleDesc tupleDesc, Tuplestorestate* tupstore, int operation)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    Datum values[WAL_RECV_WRITER_STATS_COLS];
    bool isnulls[WAL_RECV_WRITER_STATS_COLS] = {false};

    volatile WalRecvWriterStats* stats = g_instance.wal_cxt.walRecvWriterStats;
    SpinLockAcquire(&stats->mutex);
    TimestampTz curTime = GetCurrentTimestamp();
    if (operation == STAT_OPER_ENABLE) {
        stats->isEnableStat = true;
    } else if (operation == STAT_OPER_DISABLE) {
        stats->isEnableStat = false;
    } else if (operation == STAT_OPER_RESET) {
        stats->totalWriteBytes = 0;
        stats->writeTimes = 0;
        stats->totalWriteTime = 0;
        stats->totalSyncBytes = 0;
        stats->syncTimes = 0;
        stats->totalSyncTime = 0;
        stats->lastResetTime = curTime;
    }

    bool isEnableStat = stats->isEnableStat;
    uint64 totalWriteBytes = stats->totalWriteBytes;
    uint64 writeTimes = stats->writeTimes;
    uint64 totalWriteTime = stats->totalWriteTime;
    uint64 totalSyncBytes = stats->totalSyncBytes;
    uint64 syncTimes = stats->syncTimes;
    uint64 totalSyncTime = stats->totalSyncTime;
    uint64 currentXlogSegno = stats->currentXlogSegno;
    TimestampTz lastResetTime = stats->lastResetTime;
    SpinLockRelease(&stats->mutex);

    int colIdx = 0;
    /* is_enable_stat */
    values[colIdx++] = BoolGetDatum(isEnableStat);
    /* total_write_bytes */
    values[colIdx++] = UInt64GetDatum(totalWriteBytes);
    /* write_times */
    values[colIdx++] = UInt64GetDatum(writeTimes);
    /* total_write_time */
    values[colIdx++] = UInt64GetDatum(totalWriteTime);

    /*
      avg_write_time
      avg_write_bytes
    */
    uint64 avgWriteTime;
    uint64 avgWriteBytes;
    if (writeTimes > 0) {
        isnulls[colIdx] = false;
        isnulls[colIdx + 1] = false;
        avgWriteTime = totalWriteTime / writeTimes;
        avgWriteBytes = totalWriteBytes / writeTimes;
    } else {
        isnulls[colIdx] = true;
        isnulls[colIdx + 1] = true;
        avgWriteTime = 0;
        avgWriteBytes = 0;
    }
    values[colIdx++] = UInt64GetDatum(avgWriteTime);
    values[colIdx++] = UInt64GetDatum(avgWriteBytes);

    /* total_sync_bytes */
    values[colIdx++] = UInt64GetDatum(totalSyncBytes);
    /* sync_times */
    values[colIdx++] = UInt64GetDatum(syncTimes);
    /* total_sync_time */
    values[colIdx++] = UInt64GetDatum(totalWriteTime);

    /*
      avg_sync_time
      avg_sync_bytes
    */
    uint64 avgSyncBytes;
    uint64 avgSyncTime;
    if (syncTimes > 0) {
        isnulls[colIdx] = false;
        isnulls[colIdx + 1] = false;
        avgSyncTime = totalSyncTime / syncTimes;
        avgSyncBytes = totalSyncBytes / syncTimes;
    } else {
        isnulls[colIdx] = true;
        isnulls[colIdx + 1] = true;
        avgSyncTime = 0;
        avgSyncBytes = 0;
    }
    values[colIdx++] = UInt64GetDatum(avgSyncTime);
    values[colIdx++] = UInt64GetDatum(avgSyncBytes);

    /* current_xlog_segno */
    values[colIdx++] = UInt64GetDatum(currentXlogSegno);
    /* inited_xlog_segno */
    values[colIdx++] = UInt64GetDatum(GetNewestXLOGSegNo(t_thrd.proc_cxt.DataDir));
    /* last_reset_time */
    isnulls[colIdx] = (lastResetTime == 0);
    values[colIdx++] = TimestampTzGetDatum(lastResetTime);
    /* cur_time */
    values[colIdx++] = TimestampTzGetDatum(curTime);

    Assert(WAL_RECV_WRITER_STATS_COLS == colIdx);

    tuplestore_putvalues(tupstore, tupleDesc, values, isnulls);
    tuplestore_donestoring(tupstore);
}
